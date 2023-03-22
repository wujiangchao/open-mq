package com.open.openmq.broker.processor;

import com.open.openmq.client.hook.SendMessageContext;
import com.open.openmq.common.message.MessageConst;
import com.open.openmq.common.message.MessageExtBrokerInner;
import com.open.openmq.common.protocol.RequestCode;
import com.open.openmq.remoting.exception.RemotingCommandException;
import com.open.openmq.remoting.netty.NettyRequestProcessor;
import com.open.openmq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;

/**
 * @Description TODO
 * @Date 2023/3/22 14:46
 * @Author jack wu
 */
public class SendMessageProcessor extends AbstractSendMessageProcessor implements NettyRequestProcessor {
    public SendMessageProcessor() {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        SendMessageContext traceContext;
        switch (request.getCode()) {
            case RequestCode.CONSUMER_SEND_MSG_BACK:
                return this.consumerSendMsgBack(ctx, request);
            default:
                SendMessageRequestHeader requestHeader = parseRequestHeader(request);
                if (requestHeader == null) {
                    return null;
                }
                TopicQueueMappingContext mappingContext = this.brokerController.getTopicQueueMappingManager().buildTopicQueueMappingContext(requestHeader, true);
                RemotingCommand rewriteResult = this.brokerController.getTopicQueueMappingManager().rewriteRequestForStaticTopic(requestHeader, mappingContext);
                if (rewriteResult != null) {
                    return rewriteResult;
                }
                traceContext = buildMsgContext(ctx, requestHeader);
                String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
                traceContext.setCommercialOwner(owner);
                try {
                    this.executeSendMessageHookBefore(ctx, request, traceContext);
                } catch (AbortProcessException e) {
                    final RemotingCommand errorResponse = RemotingCommand.createResponseCommand(e.getResponseCode(), e.getErrorMessage());
                    errorResponse.setOpaque(request.getOpaque());
                    return errorResponse;
                }

                RemotingCommand response;
                if (requestHeader.isBatch()) {
                    response = this.sendBatchMessage(ctx, request, traceContext, requestHeader, mappingContext,
                            (ctx1, response1) -> executeSendMessageHookAfter(response1, ctx1));
                } else {
                    response = this.sendMessage(ctx, request, traceContext, requestHeader, mappingContext,
                            (ctx12, response12) -> executeSendMessageHookAfter(response12, ctx12));
                }

                return response;
        }
    }

    public RemotingCommand sendMessage(final ChannelHandlerContext ctx,
                                       final RemotingCommand request,
                                       final SendMessageContext sendMessageContext,
                                       final SendMessageRequestHeader requestHeader,
                                       final TopicQueueMappingContext mappingContext,
                                       final SendMessageCallback sendMessageCallback) throws RemotingCommandException {

        final RemotingCommand response = preSend(ctx, request, requestHeader);
        if (response.getCode() != -1) {
            return response;
        }

        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();

        final byte[] body = request.getBody();

        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

        if (queueIdInt < 0) {
            queueIdInt = randomQueueId(topicConfig.getWriteQueueNums());
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setQueueId(queueIdInt);

        Map<String, String> oriProps = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        if (!handleRetryAndDLQ(requestHeader, response, request, msgInner, topicConfig, oriProps)) {
            return response;
        }

        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());

        String uniqKey = oriProps.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        if (uniqKey == null || uniqKey.length() <= 0) {
            uniqKey = MessageClientIDSetter.createUniqID();
            oriProps.put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, uniqKey);
        }

        MessageAccessor.setProperties(msgInner, oriProps);
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(topicConfig.getTopicFilterType(), msgInner.getTags()));
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_CLUSTER, clusterName);

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        // Map<String, String> oriProps = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        String traFlag = oriProps.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        boolean sendTransactionPrepareMessage = false;
        if (Boolean.parseBoolean(traFlag)
                && !(msgInner.getReconsumeTimes() > 0 && msgInner.getDelayTimeLevel() > 0)) { //For client under version 4.6.1
            if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark(
                        "the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                                + "] sending transaction message is forbidden");
                return response;
            }
            sendTransactionPrepareMessage = true;
        }

        long beginTimeMillis = this.brokerController.getMessageStore().now();

        if (brokerController.getBrokerConfig().isAsyncSendEnable()) {
            CompletableFuture<PutMessageResult> asyncPutMessageFuture;
            if (sendTransactionPrepareMessage) {
                asyncPutMessageFuture = this.brokerController.getTransactionalMessageService().asyncPrepareMessage(msgInner);
            } else {
                asyncPutMessageFuture = this.brokerController.getMessageStore().asyncPutMessage(msgInner);
            }

            final int finalQueueIdInt = queueIdInt;
            final MessageExtBrokerInner finalMsgInner = msgInner;
            asyncPutMessageFuture.thenAcceptAsync(putMessageResult -> {
                RemotingCommand responseFuture =
                        handlePutMessageResult(putMessageResult, response, request, finalMsgInner, responseHeader, sendMessageContext,
                                ctx, finalQueueIdInt, beginTimeMillis, mappingContext);
                if (responseFuture != null) {
                    doResponse(ctx, request, responseFuture);
                }
                sendMessageCallback.onComplete(sendMessageContext, response);
            }, this.brokerController.getPutMessageFutureExecutor());
            // Returns null to release the send message thread
            return null;
        } else {
            PutMessageResult putMessageResult = null;
            if (sendTransactionPrepareMessage) {
                putMessageResult = this.brokerController.getTransactionalMessageService().prepareMessage(msgInner);
            } else {
                putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
            }
            handlePutMessageResult(putMessageResult, response, request, msgInner, responseHeader, sendMessageContext, ctx, queueIdInt, beginTimeMillis, mappingContext);
            sendMessageCallback.onComplete(sendMessageContext, response);
            return response;
        }
    }
}
