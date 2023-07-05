package com.open.openmq.client;

import com.open.openmq.client.impl.factory.MQClientInstance;
import com.open.openmq.client.log.ClientLogger;
import com.open.openmq.logging.InternalLogger;
import com.open.openmq.remoting.netty.NettyRequestProcessor;
import com.open.openmq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

/**
 * @Description TODO
 * @Date 2023/5/10 16:24
 * @Author jack wu
 */
public class ClientRemotingProcessor implements NettyRequestProcessor {

    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mqClientFactory;

    public ClientRemotingProcessor(final MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
