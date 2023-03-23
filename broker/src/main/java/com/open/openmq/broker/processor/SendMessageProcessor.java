package com.open.openmq.broker.processor;

import com.open.openmq.client.hook.SendMessageContext;
import com.open.openmq.common.message.MessageConst;
import com.open.openmq.common.message.MessageExt;
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

    private RemotingCommand handlePutMessageResult(PutMessageResult putMessageResult, RemotingCommand response,
                                                   RemotingCommand request, MessageExt msg,
                                                   SendMessageResponseHeader responseHeader, SendMessageContext sendMessageContext, ChannelHandlerContext ctx,
                                                   int queueIdInt, long beginTimeMillis, TopicQueueMappingContext mappingContext) {
        if (putMessageResult == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
            return response;
        }
        boolean sendOK = false;

        switch (putMessageResult.getPutMessageStatus()) {
            // Success
            case PUT_OK:
                sendOK = true;
                response.setCode(ResponseCode.SUCCESS);
                break;
            case FLUSH_DISK_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_DISK_TIMEOUT);
                sendOK = true;
                break;
            case FLUSH_SLAVE_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_SLAVE_TIMEOUT);
                sendOK = true;
                break;
            case SLAVE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
                sendOK = true;
                break;

            // Failed
            case IN_SYNC_REPLICAS_NOT_ENOUGH:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("in-sync replicas not enough");
                break;
            case CREATE_MAPPED_FILE_FAILED:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("create mapped file failed, server is busy or broken.");
                break;
            case MESSAGE_ILLEGAL:
            case PROPERTIES_SIZE_EXCEEDED:
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(String.format("the message is illegal, maybe msg body or properties length not matched. msg body length limit %dB, msg properties length limit 32KB.",
                        this.brokerController.getMessageStoreConfig().getMaxMessageSize()));
                break;
            case WHEEL_TIMER_MSG_ILLEGAL:
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(String.format("timer message illegal, the delay time should not be bigger than the max delay %dms; or if set del msg, the delay time should be bigger than the current time",
                        this.brokerController.getMessageStoreConfig().getTimerMaxDelaySec() * 1000));
                break;
            case WHEEL_TIMER_FLOW_CONTROL:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(String.format("timer message is under flow control, max num limit is %d or the current value is greater than %d and less than %d, trigger random flow control",
                        this.brokerController.getMessageStoreConfig().getTimerCongestNumEachSlot() * 2L, this.brokerController.getMessageStoreConfig().getTimerCongestNumEachSlot(), this.brokerController.getMessageStoreConfig().getTimerCongestNumEachSlot() * 2L));
                break;
            case WHEEL_TIMER_NOT_ENABLE:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(String.format("accurate timer message is not enabled, timerWheelEnable is %s",
                        this.brokerController.getMessageStoreConfig().isTimerWheelEnable()));
            case SERVICE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                response.setRemark(
                        "service not available now. It may be caused by one of the following reasons: " +
                                "the broker's disk is full [" + diskUtil() + "], messages are put to the slave, message store has been shut down, etc.");
                break;
            case OS_PAGE_CACHE_BUSY:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("[PC_SYNCHRONIZED]broker busy, start flow control for a while");
                break;
            case LMQ_CONSUME_QUEUE_NUM_EXCEEDED:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("[LMQ_CONSUME_QUEUE_NUM_EXCEEDED]broker config enableLmq and enableMultiDispatch, lmq consumeQueue num exceed maxLmqConsumeQueueNum config num, default limit 2w.");
                break;
            case UNKNOWN_ERROR:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR");
                break;
            default:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR DEFAULT");
                break;
        }

        String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
        String authType = request.getExtFields().get(BrokerStatsManager.ACCOUNT_AUTH_TYPE);
        String ownerParent = request.getExtFields().get(BrokerStatsManager.ACCOUNT_OWNER_PARENT);
        String ownerSelf = request.getExtFields().get(BrokerStatsManager.ACCOUNT_OWNER_SELF);
        int commercialSizePerMsg = brokerController.getBrokerConfig().getCommercialSizePerMsg();
        if (sendOK) {

            if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(msg.getTopic())) {
                this.brokerController.getBrokerStatsManager().incQueuePutNums(msg.getTopic(), msg.getQueueId(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
                this.brokerController.getBrokerStatsManager().incQueuePutSize(msg.getTopic(), msg.getQueueId(), putMessageResult.getAppendMessageResult().getWroteBytes());
            }

            this.brokerController.getBrokerStatsManager().incTopicPutNums(msg.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
            this.brokerController.getBrokerStatsManager().incTopicPutSize(msg.getTopic(),
                    putMessageResult.getAppendMessageResult().getWroteBytes());
            this.brokerController.getBrokerStatsManager().incBrokerPutNums(putMessageResult.getAppendMessageResult().getMsgNum());
            this.brokerController.getBrokerStatsManager().incTopicPutLatency(msg.getTopic(), queueIdInt,
                    (int) (this.brokerController.getMessageStore().now() - beginTimeMillis));

            response.setRemark(null);

            responseHeader.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            responseHeader.setQueueId(queueIdInt);
            responseHeader.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());
            responseHeader.setTransactionId(MessageClientIDSetter.getUniqID(msg));

            RemotingCommand rewriteResult = rewriteResponseForStaticTopic(responseHeader, mappingContext);
            if (rewriteResult != null) {
                return rewriteResult;
            }

            doResponse(ctx, request, response);

            if (hasSendMessageHook()) {
                sendMessageContext.setMsgId(responseHeader.getMsgId());
                sendMessageContext.setQueueId(responseHeader.getQueueId());
                sendMessageContext.setQueueOffset(responseHeader.getQueueOffset());

                int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
                int wroteSize = putMessageResult.getAppendMessageResult().getWroteBytes();
                int msgNum = putMessageResult.getAppendMessageResult().getMsgNum();
                int commercialMsgNum = (int) Math.ceil(wroteSize / (double) commercialSizePerMsg);
                int incValue = commercialMsgNum * commercialBaseCount;

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_SUCCESS);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);

                sendMessageContext.setSendStat(BrokerStatsManager.StatsType.SEND_SUCCESS);
                sendMessageContext.setCommercialSendMsgNum(commercialMsgNum);
                sendMessageContext.setAccountAuthType(authType);
                sendMessageContext.setAccountOwnerParent(ownerParent);
                sendMessageContext.setAccountOwnerSelf(ownerSelf);
                sendMessageContext.setSendMsgSize(wroteSize);
                sendMessageContext.setSendMsgNum(msgNum);
            }
            return null;
        } else {
            if (hasSendMessageHook()) {
                AppendMessageResult appendMessageResult = putMessageResult.getAppendMessageResult();

                // TODO process partial failures of batch message
                int wroteSize = request.getBody().length;
                int msgNum = Math.max(appendMessageResult != null ? appendMessageResult.getMsgNum() : 1, 1);
                int commercialMsgNum = (int) Math.ceil(wroteSize / (double) commercialSizePerMsg);
                int incValue = commercialMsgNum;

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_FAILURE);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);

                sendMessageContext.setSendStat(BrokerStatsManager.StatsType.SEND_FAILURE);
                sendMessageContext.setCommercialSendMsgNum(commercialMsgNum);
                sendMessageContext.setAccountAuthType(authType);
                sendMessageContext.setAccountOwnerParent(ownerParent);
                sendMessageContext.setAccountOwnerSelf(ownerSelf);
                sendMessageContext.setSendMsgSize(wroteSize);
                sendMessageContext.setSendMsgNum(msgNum);
            }
        }
        return response;
    }
}