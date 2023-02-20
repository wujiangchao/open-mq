package com.open.openmq.client.impl;

import com.open.openmq.client.exception.MQBrokerException;
import com.open.openmq.client.exception.MQClientException;
import com.open.openmq.client.hook.SendMessageContext;
import com.open.openmq.client.impl.factory.MQClientInstance;
import com.open.openmq.client.impl.producer.DefaultMQProducerImpl;
import com.open.openmq.client.impl.producer.SendCallback;
import com.open.openmq.client.impl.producer.TopicPublishInfo;
import com.open.openmq.client.producer.SendResult;
import com.open.openmq.common.MixAll;
import com.open.openmq.common.message.Message;
import com.open.openmq.common.message.MessageConst;
import com.open.openmq.common.protocol.route.TopicRouteData;
import com.open.openmq.remoting.exception.RemotingConnectException;
import com.open.openmq.remoting.exception.RemotingException;
import com.open.openmq.remoting.exception.RemotingSendRequestException;
import com.open.openmq.remoting.exception.RemotingTimeoutException;
import com.open.openmq.remoting.exception.RemotingTooMuchRequestException;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Description TODO
 * @Date 2023/2/15 22:06
 * @Author jack wu
 */
public class MQClientAPIImpl {

    public SendResult sendMessage(
            final String addr,
            final String brokerName,
            final Message msg,
            final SendMessageRequestHeader requestHeader,
            final long timeoutMillis,
            final CommunicationMode communicationMode,
            final SendCallback sendCallback,
            final TopicPublishInfo topicPublishInfo,
            final MQClientInstance instance,
            final int retryTimesWhenSendFailed,
            final SendMessageContext context,
            final DefaultMQProducerImpl producer
    ) throws RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        RemotingCommand request = null;
        String msgType = msg.getProperty(MessageConst.PROPERTY_MESSAGE_TYPE);
        boolean isReply = msgType != null && msgType.equals(MixAll.REPLY_MESSAGE_FLAG);
        if (isReply) {
            if (sendSmartMsg) {
                SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_REPLY_MESSAGE_V2, requestHeaderV2);
            } else {
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_REPLY_MESSAGE, requestHeader);
            }
        } else {
            if (sendSmartMsg || msg instanceof MessageBatch) {
                SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
                request = RemotingCommand.createRequestCommand(msg instanceof MessageBatch ? RequestCode.SEND_BATCH_MESSAGE : RequestCode.SEND_MESSAGE_V2, requestHeaderV2);
            } else {
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
            }
        }
        request.setBody(msg.getBody());

        switch (communicationMode) {
            case ONEWAY:
                this.remotingClient.invokeOneway(addr, request, timeoutMillis);
                return null;
            case ASYNC:
                final AtomicInteger times = new AtomicInteger();
                long costTimeAsync = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTimeAsync) {
                    throw new RemotingTooMuchRequestException("sendMessage call timeout");
                }
                this.sendMessageAsync(addr, brokerName, msg, timeoutMillis - costTimeAsync, request, sendCallback, topicPublishInfo, instance,
                        retryTimesWhenSendFailed, times, context, producer);
                return null;
            case SYNC:
                long costTimeSync = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTimeSync) {
                    throw new RemotingTooMuchRequestException("sendMessage call timeout");
                }
                return this.sendMessageSync(addr, brokerName, msg, timeoutMillis - costTimeSync, request);
            default:
                assert false;
                break;
        }

        return null;
    }

    public TopicRouteData getDefaultTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {

        return getTopicRouteInfoFromNameServer(topic, timeoutMillis, false);
    }

    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis,
                                                          boolean allowTopicNotExist) throws MQClientException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.TOPIC_NOT_EXIST: {
                if (allowTopicNotExist) {
                    log.warn("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
                }

                break;
            }
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return TopicRouteData.decode(body, TopicRouteData.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }


}
