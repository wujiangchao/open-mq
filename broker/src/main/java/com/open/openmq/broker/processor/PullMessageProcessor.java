package com.open.openmq.broker.processor;

import com.open.openmq.broker.BrokerController;
import com.open.openmq.broker.client.ConsumerGroupInfo;
import com.open.openmq.common.MixAll;
import com.open.openmq.common.TopicConfig;
import com.open.openmq.common.filter.ExpressionType;
import com.open.openmq.common.protocol.RequestCode;
import com.open.openmq.common.protocol.ResponseCode;
import com.open.openmq.common.protocol.header.PullMessageRequestHeader;
import com.open.openmq.common.protocol.header.PullMessageResponseHeader;
import com.open.openmq.common.protocol.heartbeat.MessageModel;
import com.open.openmq.common.protocol.heartbeat.SubscriptionData;
import com.open.openmq.remoting.exception.RemotingCommandException;
import com.open.openmq.remoting.netty.NettyRequestProcessor;
import com.open.openmq.remoting.protocol.RemotingCommand;
import com.open.openmq.store.GetMessageResult;
import com.open.openmq.store.GetMessageStatus;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

/**
 * @Description TODO
 * @Date 2023/3/24 13:17
 * @Author jack wu
 */
public class PullMessageProcessor implements NettyRequestProcessor {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        //调用另一个processRequest方法，第三个参数broker是否支持挂起请求为true
        return this.processRequest(ctx.channel(), request, true);
    }

    private RemotingCommand processRequest(final Channel channel, RemotingCommand request, boolean brokerAllowSuspend)
            throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);
        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();
        final PullMessageRequestHeader requestHeader =
                (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);

        response.setOpaque(request.getOpaque());

        LOGGER.debug("receive PullMessage request command, {}", request);

        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] pulling message is forbidden", this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        if (request.getCode() == RequestCode.LITE_PULL_MESSAGE && !this.brokerController.getBrokerConfig().isLitePullMessageEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(
                    "the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1() + "] for lite pull consumer is forbidden");
            return response;
        }

        //获取当前consumerGroup对应的订阅信息
        SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("subscription group [%s] does not exist, %s", requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }

        //判断是否可消费，不可消费直接返回
        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            responseHeader.setForbiddenType(ForbiddenType.GROUP_FORBIDDEN);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }

        //是否提交消费进度
        final boolean hasCommitOffsetFlag = PullSysFlag.hasCommitOffsetFlag(requestHeader.getSysFlag());
        //是否存在子订阅，即TAG或者SQL92的设置，用于过滤消息
        final boolean hasSubscriptionFlag = PullSysFlag.hasSubscriptionFlag(requestHeader.getSysFlag());

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            LOGGER.error("the topic {} not exist, consumer: {}", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        //topic是否可读，不可读直接返回
        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            responseHeader.setForbiddenType(ForbiddenType.TOPIC_FORBIDDEN);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] pulling message is forbidden");
            return response;
        }

        TopicQueueMappingContext mappingContext = this.brokerController.getTopicQueueMappingManager().buildTopicQueueMappingContext(requestHeader, false);

        {
            RemotingCommand rewriteResult = rewriteRequestForStaticTopic(requestHeader, mappingContext);
            if (rewriteResult != null) {
                return rewriteResult;
            }
        }

        //校验请求中的队列id，如果小于0或者大于等于topic配置中的读队列数量，那么直接返回
        if (requestHeader.getQueueId() < 0 || requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                    requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
            LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return response;
        }

        /**
         * 1. 构建过滤消息
         * 真正的过滤消息操作还在后面，而且broker和consumer都会进行过滤
         */
        SubscriptionData subscriptionData = null;
        ConsumerFilterData consumerFilterData = null;
        if (hasSubscriptionFlag) {
            try {
                subscriptionData = FilterAPI.build(
                        requestHeader.getTopic(), requestHeader.getSubscription(), requestHeader.getExpressionType()
                );
                if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    consumerFilterData = ConsumerFilterManager.build(
                            requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getSubscription(),
                            requestHeader.getExpressionType(), requestHeader.getSubVersion()
                    );
                    assert consumerFilterData != null;
                }
            } catch (Exception e) {
                LOGGER.warn("Parse the consumer's subscription[{}] failed, group: {}", requestHeader.getSubscription(),
                        requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark("parse the consumer's subscription failed");
                return response;
            }
        } else {
            //获取消费者组的信息
            ConsumerGroupInfo consumerGroupInfo =
                    this.brokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup());
            if (null == consumerGroupInfo) {
                LOGGER.warn("the consumer's group info not exist, group: {}", requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's group info not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }

            //如果不支持广播消费但是消费者消费模是的广播消费，则直接返回
            if (!subscriptionGroupConfig.isConsumeBroadcastEnable()
                    && consumerGroupInfo.getMessageModel() == MessageModel.BROADCASTING) {
                response.setCode(ResponseCode.NO_PERMISSION);
                responseHeader.setForbiddenType(ForbiddenType.BROADCASTING_DISABLE_FORBIDDEN);
                response.setRemark("the consumer group[" + requestHeader.getConsumerGroup() + "] can not consume by broadcast way");
                return response;
            }

            boolean readForbidden = this.brokerController.getSubscriptionGroupManager().getForbidden(
                    subscriptionGroupConfig.getGroupName(), requestHeader.getTopic(), PermName.INDEX_PERM_READ);
            if (readForbidden) {
                response.setCode(ResponseCode.NO_PERMISSION);
                responseHeader.setForbiddenType(ForbiddenType.SUBSCRIPTION_FORBIDDEN);
                response.setRemark("the consumer group[" + requestHeader.getConsumerGroup() + "] is forbidden for topic[" + requestHeader.getTopic() + "]");
                return response;
            }

            //获取broker缓存的此consumerGroupInfo中关于此topic的订阅关系，这个就是consumer启动时发送给broker的
            subscriptionData = consumerGroupInfo.findSubscriptionData(requestHeader.getTopic());
            if (null == subscriptionData) {
                LOGGER.warn("the consumer's subscription not exist, group: {}, topic:{}", requestHeader.getConsumerGroup(), requestHeader.getTopic());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's subscription not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }

            //比较订阅关系版本
            if (subscriptionData.getSubVersion() < requestHeader.getSubVersion()) {
                LOGGER.warn("The broker's subscription is not latest, group: {} {}", requestHeader.getConsumerGroup(),
                        subscriptionData.getSubString());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_LATEST);
                response.setRemark("the consumer's subscription not latest");
                return response;
            }
            if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                consumerFilterData = this.brokerController.getConsumerFilterManager().get(requestHeader.getTopic(),
                        requestHeader.getConsumerGroup());
                if (consumerFilterData == null) {
                    response.setCode(ResponseCode.FILTER_DATA_NOT_EXIST);
                    response.setRemark("The broker's consumer filter data is not exist!Your expression may be wrong!");
                    return response;
                }
                if (consumerFilterData.getClientVersion() < requestHeader.getSubVersion()) {
                    LOGGER.warn("The broker's consumer filter data is not latest, group: {}, topic: {}, serverV: {}, clientV: {}",
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(), consumerFilterData.getClientVersion(), requestHeader.getSubVersion());
                    response.setCode(ResponseCode.FILTER_DATA_NOT_LATEST);
                    response.setRemark("the consumer's consumer filter data not latest");
                    return response;
                }
            }
        }

        if (!ExpressionType.isTagType(subscriptionData.getExpressionType())
                && !this.brokerController.getBrokerConfig().isEnablePropertyFilter()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The broker does not support consumer to filter message by " + subscriptionData.getExpressionType());
            return response;
        }

        MessageFilter messageFilter;
        if (this.brokerController.getBrokerConfig().isFilterSupportRetry()) {
            messageFilter = new ExpressionForRetryMessageFilter(subscriptionData, consumerFilterData,
                    this.brokerController.getConsumerFilterManager());
        } else {
            messageFilter = new ExpressionMessageFilter(subscriptionData, consumerFilterData,
                    this.brokerController.getConsumerFilterManager());
        }

        /**
         * 2 通过DefaultMessageStore#getMessage方法批量拉取消息，并且进行过滤操作。
         */
        final GetMessageResult getMessageResult =
                this.brokerController.getMessageStore().getMessage(requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                        requestHeader.getQueueId(), requestHeader.getQueueOffset(), requestHeader.getMaxMsgNums(), messageFilter);
        /**
         * 3 对于拉取结果GetMessageResult进行处理
         */
        if (getMessageResult != null) {
            //设置拉取状态枚举名称
            response.setRemark(getMessageResult.getStatus().name());
            //设置下次拉取的consumeQueue的起始逻辑偏移量
            responseHeader.setNextBeginOffset(getMessageResult.getNextBeginOffset());
            //设置consumeQueue的最小、最大的逻辑偏移量maxOffset 和 minOffset
            responseHeader.setMinOffset(getMessageResult.getMinOffset());
            // this does not need to be modified since it's not an accurate value under logical queue.
            responseHeader.setMaxOffset(getMessageResult.getMaxOffset());
            responseHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
            responseHeader.setGroupSysFlag(subscriptionGroupConfig.getGroupSysFlag());

            /**
             * 3.1 判断拉取消息状态码，并设置对应的响应码
             */
            switch (getMessageResult.getStatus()) {
                case FOUND:
                    response.setCode(ResponseCode.SUCCESS);
                    break;
                case MESSAGE_WAS_REMOVING:
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    break;
                case NO_MATCHED_LOGIC_QUEUE:
                case NO_MESSAGE_IN_QUEUE:
                    if (0 != requestHeader.getQueueOffset()) {
                        response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                        // XXX: warn and notify me
                        LOGGER.info("the broker stores no queue data, fix the request offset {} to {}, Topic: {} QueueId: {} Consumer Group: {}",
                                requestHeader.getQueueOffset(),
                                getMessageResult.getNextBeginOffset(),
                                requestHeader.getTopic(),
                                requestHeader.getQueueId(),
                                requestHeader.getConsumerGroup()
                        );
                    } else {
                        response.setCode(ResponseCode.PULL_NOT_FOUND);
                    }
                    break;
                case NO_MATCHED_MESSAGE:
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    break;
                case OFFSET_FOUND_NULL:
                    response.setCode(ResponseCode.PULL_NOT_FOUND);
                    break;
                case OFFSET_OVERFLOW_BADLY:
                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    // XXX: warn and notify me
                    LOGGER.info("the request offset: {} over flow badly, fix to {}, broker max offset: {}, consumer: {}",
                            requestHeader.getQueueOffset(), getMessageResult.getNextBeginOffset(), getMessageResult.getMaxOffset(), channel.remoteAddress());
                    break;
                case OFFSET_OVERFLOW_ONE:
                    response.setCode(ResponseCode.PULL_NOT_FOUND);
                    break;
                case OFFSET_TOO_SMALL:
                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    LOGGER.info("the request offset too small. group={}, topic={}, requestOffset={}, brokerMinOffset={}, clientIp={}",
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueOffset(),
                            getMessageResult.getMinOffset(), channel.remoteAddress());
                    break;
                default:
                    assert false;
                    break;
            }

            if (this.brokerController.getBrokerConfig().isSlaveReadEnable() && !this.brokerController.getBrokerConfig().isInBrokerContainer()) {
                // consume too slow ,redirect to another machine
                if (getMessageResult.isSuggestPullingFromSlave()) {
                    responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly());
                }
                // consume ok
                else {
                    responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                }
            } else {
                responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
            }

            if (this.brokerController.getBrokerConfig().getBrokerId() != MixAll.MASTER_ID && !getMessageResult.isSuggestPullingFromSlave()) {
                if (this.brokerController.getMinBrokerIdInGroup() == MixAll.MASTER_ID) {
                    LOGGER.debug("slave redirect pullRequest to master, topic: {}, queueId: {}, consumer group: {}, next: {}, min: {}, max: {}",
                            requestHeader.getTopic(),
                            requestHeader.getQueueId(),
                            requestHeader.getConsumerGroup(),
                            responseHeader.getNextBeginOffset(),
                            responseHeader.getMinOffset(),
                            responseHeader.getMaxOffset()
                    );
                    responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
                    if (!getMessageResult.getStatus().equals(GetMessageStatus.FOUND)) {
                        response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    }
                }
            }

            /**
             * 3.3 判断如果有消费钩子，那么执行consumeMessageBefore方法
             */
            if (this.hasConsumeMessageHook()) {
                String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
                String authType = request.getExtFields().get(BrokerStatsManager.ACCOUNT_AUTH_TYPE);
                String ownerParent = request.getExtFields().get(BrokerStatsManager.ACCOUNT_OWNER_PARENT);
                String ownerSelf = request.getExtFields().get(BrokerStatsManager.ACCOUNT_OWNER_SELF);

                //构建上下文信息
                ConsumeMessageContext context = new ConsumeMessageContext();
                context.setConsumerGroup(requestHeader.getConsumerGroup());
                context.setTopic(requestHeader.getTopic());
                context.setQueueId(requestHeader.getQueueId());
                context.setAccountAuthType(authType);
                context.setAccountOwnerParent(ownerParent);
                context.setAccountOwnerSelf(ownerSelf);
                context.setNamespace(NamespaceUtil.getNamespaceFromResource(requestHeader.getTopic()));

                //3.4 判断响应码，然后直接返回数据或者进行短轮询或者长轮询
                switch (response.getCode()) {
                    case ResponseCode.SUCCESS:
                        int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
                        int incValue = getMessageResult.getMsgCount4Commercial() * commercialBaseCount;

                        context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_SUCCESS);
                        context.setCommercialRcvTimes(incValue);
                        context.setCommercialRcvSize(getMessageResult.getBufferTotalSize());
                        context.setCommercialOwner(owner);

                        context.setRcvStat(BrokerStatsManager.StatsType.RCV_SUCCESS);
                        context.setRcvMsgNum(getMessageResult.getMessageCount());
                        context.setRcvMsgSize(getMessageResult.getBufferTotalSize());
                        context.setCommercialRcvMsgNum(getMessageResult.getMsgCount4Commercial());

                        break;
                    case ResponseCode.PULL_NOT_FOUND:
                        if (!brokerAllowSuspend) {

                            context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_EPOLLS);
                            context.setCommercialRcvTimes(1);
                            context.setCommercialOwner(owner);

                            context.setRcvStat(BrokerStatsManager.StatsType.RCV_EPOLLS);
                            context.setRcvMsgNum(0);
                            context.setRcvMsgSize(0);
                            context.setCommercialRcvMsgNum(0);
                        }
                        break;
                    case ResponseCode.PULL_RETRY_IMMEDIATELY:
                    case ResponseCode.PULL_OFFSET_MOVED:
                        context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_EPOLLS);
                        context.setCommercialRcvTimes(1);
                        context.setCommercialOwner(owner);

                        context.setRcvStat(BrokerStatsManager.StatsType.RCV_EPOLLS);
                        context.setRcvMsgNum(0);
                        context.setRcvMsgSize(0);
                        context.setCommercialRcvMsgNum(0);
                        break;
                    default:
                        assert false;
                        break;
                }

                try {
                    this.executeConsumeMessageHookBefore(context);
                } catch (AbortProcessException e) {
                    response.setCode(e.getResponseCode());
                    response.setRemark(e.getErrorMessage());
                    return response;
                }
            }

            //rewrite the response for the
            RemotingCommand rewriteResult = rewriteResponseForStaticTopic(requestHeader, responseHeader, mappingContext, response.getCode());
            if (rewriteResult != null) {
                response = rewriteResult;
            }

            response = this.pullMessageResultHandler.handle(
                    getMessageResult,
                    request,
                    requestHeader,
                    channel,
                    subscriptionData,
                    subscriptionGroupConfig,
                    brokerAllowSuspend,
                    messageFilter,
                    response
            );
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store getMessage return null");
        }

        /**
         * 4. 拉取消息完毕之后， 无论是否拉取到消息，只需broker支持挂起请求，并且当前broker不是SLAVE角色，都会上报该消费者上一次的消费点位
         * 另外消费者客户端也会定时5s上报一次消费点位
         */
        //要求brokerAllowSuspend为true，新的拉取请求为true，但是已被suspend的请求将会是false
        boolean storeOffsetEnable = brokerAllowSuspend;
        storeOffsetEnable = storeOffsetEnable && hasCommitOffsetFlag;
        if (storeOffsetEnable) {
            this.brokerController.getConsumerOffsetManager().commitOffset(RemotingHelper.parseChannelRemoteAddr(channel),
                    requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
        }
        return response;
    }


    @Override
    public boolean rejectRequest() {
        if (!this.brokerController.getBrokerConfig().isSlaveReadEnable()
                && this.brokerController.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
            return true;
        }
        return false;
    }

}
