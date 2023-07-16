package com.open.openmq.namesrv.processor;

import com.alibaba.fastjson.serializer.SerializerFeature;
import com.open.openmq.common.namesrv.NamesrvUtil;
import com.open.openmq.common.protocol.ResponseCode;
import com.open.openmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import com.open.openmq.common.protocol.route.TopicRouteData;
import com.open.openmq.namesrv.NamesrvController;
import com.open.openmq.remoting.exception.RemotingCommandException;
import com.open.openmq.remoting.netty.NettyRequestProcessor;
import com.open.openmq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

/**
 * @Description 处理客户端请求， 目前包含获取路由信息
 * @Date 2023/2/21 22:12
 * @Author jack wu
 */
public class ClientRequestProcessor implements NettyRequestProcessor {

    protected NamesrvController namesrvController;

    public ClientRequestProcessor(final NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
                                          final RemotingCommand request) throws Exception {
        return this.getRouteInfoByTopic(ctx, request);
    }

    public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx,
                                               RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetRouteInfoRequestHeader requestHeader =
                (GetRouteInfoRequestHeader) request.decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);

        TopicRouteData topicRouteData = this.namesrvController.getRouteInfoManager().pickupTopicRouteData(requestHeader.getTopic());

        if (topicRouteData != null) {
            if (this.namesrvController.getNamesrvConfig().isOrderMessageEnable()) {
                String orderTopicConf =
                        this.namesrvController.getKvConfigManager().getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG,
                                requestHeader.getTopic());
                topicRouteData.setOrderTopicConf(orderTopicConf);
            }

            byte[] content;
            Boolean standardJsonOnly = requestHeader.getAcceptStandardJsonOnly();
            if ( null != standardJsonOnly && standardJsonOnly) {
                content = topicRouteData.encode(SerializerFeature.BrowserCompatible,
                        SerializerFeature.QuoteFieldNames, SerializerFeature.SkipTransientField,
                        SerializerFeature.MapSortField);
            } else {
                content = topicRouteData.encode();
            }

            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        response.setRemark("No topic route info in name server for the topic: " + requestHeader.getTopic());
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
