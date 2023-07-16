package com.open.openmq.namesrv.processor;

import com.open.openmq.common.MixAll;
import com.open.openmq.common.UtilAll;
import com.open.openmq.common.constant.LoggerName;
import com.open.openmq.common.namesrv.NamesrvUtil;
import com.open.openmq.common.namesrv.RegisterBrokerResult;
import com.open.openmq.common.protocol.RequestCode;
import com.open.openmq.common.protocol.ResponseCode;
import com.open.openmq.common.protocol.body.RegisterBrokerBody;
import com.open.openmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.open.openmq.common.protocol.header.namesrv.BrokerHeartbeatRequestHeader;
import com.open.openmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import com.open.openmq.common.protocol.header.namesrv.RegisterBrokerResponseHeader;
import com.open.openmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import com.open.openmq.logging.InternalLogger;
import com.open.openmq.logging.InternalLoggerFactory;
import com.open.openmq.namesrv.NamesrvController;
import com.open.openmq.remoting.common.RemotingHelper;
import com.open.openmq.remoting.exception.RemotingCommandException;
import com.open.openmq.remoting.netty.NettyRequestProcessor;
import com.open.openmq.remoting.protocol.RemotingCommand;
import com.open.openmq.remoting.protocol.RemotingSysResponseCode;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Description 处理比如KV配置管理、Broker注册、Broker心跳、更新/查询Namesrv配置
 * 等Namesrv的请求，
 * @Date 2023/2/21 22:13
 * @Author jack wu
 */
public class DefaultRequestProcessor implements NettyRequestProcessor {

    private static InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    protected final NamesrvController namesrvController;

    public DefaultRequestProcessor(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        if (ctx != null) {
            log.debug("receive request, {} {} {}",
                    request.getCode(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                    request);
        }

        switch (request.getCode()) {
            case RequestCode.PUT_KV_CONFIG:
//                return this.putKVConfig(ctx, request);
//            case RequestCode.GET_KV_CONFIG:
//                return this.getKVConfig(ctx, request);
//            case RequestCode.DELETE_KV_CONFIG:
//                return this.deleteKVConfig(ctx, request);
//            case RequestCode.QUERY_DATA_VERSION:
//                return this.queryBrokerTopicConfig(ctx, request);
                //注册Broker
            case RequestCode.REGISTER_BROKER:
                return this.registerBroker(ctx, request);
            //注销Broker
            case RequestCode.UNREGISTER_BROKER:
                return this.unregisterBroker(ctx, request);
            //Broker心跳
            case RequestCode.BROKER_HEARTBEAT:
                return this.brokerHeartbeat(ctx, request);
//            case RequestCode.GET_BROKER_MEMBER_GROUP:
//                return this.getBrokerMemberGroup(ctx, request);
//            case RequestCode.GET_BROKER_CLUSTER_INFO:
//                return this.getBrokerClusterInfo(ctx, request);
//            case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
//                return this.wipeWritePermOfBroker(ctx, request);
//            case RequestCode.ADD_WRITE_PERM_OF_BROKER:
//                return this.addWritePermOfBroker(ctx, request);
//            case RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER:
//                return this.getAllTopicListFromNameserver(ctx, request);
//            case RequestCode.DELETE_TOPIC_IN_NAMESRV:
//                return this.deleteTopicInNamesrv(ctx, request);
//            case RequestCode.REGISTER_TOPIC_IN_NAMESRV:
//                return this.registerTopicToNamesrv(ctx, request);
//            case RequestCode.GET_KVLIST_BY_NAMESPACE:
//                return this.getKVListByNamespace(ctx, request);
//            case RequestCode.GET_TOPICS_BY_CLUSTER:
//                return this.getTopicsByCluster(ctx, request);
//            case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS:
//                return this.getSystemTopicListFromNs(ctx, request);
//            case RequestCode.GET_UNIT_TOPIC_LIST:
//                return this.getUnitTopicList(ctx, request);
//            case RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST:
//                return this.getHasUnitSubTopicList(ctx, request);
//            case RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST:
//                return this.getHasUnitSubUnUnitTopicList(ctx, request);
//            case RequestCode.UPDATE_NAMESRV_CONFIG:
//                return this.updateConfig(ctx, request);
//            case RequestCode.GET_NAMESRV_CONFIG:
//                return this.getConfig(ctx, request);
//            case RequestCode.GET_CLIENT_CONFIG:
//                return this.getClientConfigs(ctx, request);
            default:
                String error = " request type " + request.getCode() + " not supported";
                return RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
        }
    }

    public RemotingCommand registerBroker(ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {
        //创建响应命令
        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        final RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader) response.readCustomHeader();
        final RegisterBrokerRequestHeader requestHeader =
                (RegisterBrokerRequestHeader) request.decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);

        //校验crc
        if (!checksum(ctx, request, requestHeader)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("crc32 not match");
            return response;
        }

        final RegisterBrokerBody registerBrokerBody = extractRegisterBrokerBodyFromRequest(request, requestHeader);
        //获取请求中的主题配置
        TopicConfigSerializeWrapper topicConfigWrapper = registerBrokerBody.getTopicConfigSerializeWrapper();
        List<String> filterServerList = registerBrokerBody.getFilterServerList();

        RegisterBrokerResult result = this.namesrvController.getRouteInfoManager().registerBroker(
                requestHeader.getClusterName(),
                requestHeader.getBrokerAddr(),
                requestHeader.getBrokerName(),
                requestHeader.getBrokerId(),
                requestHeader.getHaServerAddr(),
                request.getExtFields().get(MixAll.ZONE_NAME),
                requestHeader.getHeartbeatTimeoutMillis(),
                requestHeader.getEnableActingMaster(),
                topicConfigWrapper,
                filterServerList,
                ctx.channel()
        );

        if (result == null) {
            // Register single topic route info should be after the broker completes the first registration.
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("register broker failed");
            return response;
        }

        responseHeader.setHaServerAddr(result.getHaServerAddr());
        responseHeader.setMasterAddr(result.getMasterAddr());

        //此配置和 有序消息有关，
        if (this.namesrvController.getNamesrvConfig().isReturnOrderTopicConfigToBroker()) {
            byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
            response.setBody(jsonValue);
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RegisterBrokerBody extractRegisterBrokerBodyFromRequest(RemotingCommand request,
                                                                    RegisterBrokerRequestHeader requestHeader) throws RemotingCommandException {
        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();

        if (request.getBody() != null) {
            try {
                registerBrokerBody = RegisterBrokerBody.decode(request.getBody(), requestHeader.isCompressed());
            } catch (Exception e) {
                throw new RemotingCommandException("Failed to decode RegisterBrokerBody", e);
            }
        } else {
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setCounter(new AtomicLong(0));
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setTimestamp(0L);
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setStateVersion(0L);
        }
        return registerBrokerBody;
    }

    /**
     * 校验消息的完整性
     *
     * @param ctx
     * @param request
     * @param requestHeader
     * @return
     */
    private boolean checksum(ChannelHandlerContext ctx, RemotingCommand request,
                             RegisterBrokerRequestHeader requestHeader) {
        if (requestHeader.getBodyCrc32() != 0) {
            final int crc32 = UtilAll.crc32(request.getBody());
            if (crc32 != requestHeader.getBodyCrc32()) {
                log.warn(String.format("receive registerBroker request,crc32 not match,from %s",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel())));
                return false;
            }
        }
        return true;
    }


    public RemotingCommand unregisterBroker(ChannelHandlerContext ctx,
                                            RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final UnRegisterBrokerRequestHeader requestHeader = (UnRegisterBrokerRequestHeader) request.decodeCommandCustomHeader(UnRegisterBrokerRequestHeader.class);

        if (!this.namesrvController.getRouteInfoManager().submitUnRegisterBrokerRequest(requestHeader)) {
            log.warn("Couldn't submit the unregister broker request to handler, broker info: {}", requestHeader);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(null);
            return response;
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand brokerHeartbeat(ChannelHandlerContext ctx,
                                           RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final BrokerHeartbeatRequestHeader requestHeader =
                (BrokerHeartbeatRequestHeader) request.decodeCommandCustomHeader(BrokerHeartbeatRequestHeader.class);


        this.namesrvController.getRouteInfoManager().updateBrokerInfoUpdateTimestamp(requestHeader.getClusterName(), requestHeader.getBrokerAddr());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }


    @Override
    public boolean rejectRequest() {
        return false;
    }
}
