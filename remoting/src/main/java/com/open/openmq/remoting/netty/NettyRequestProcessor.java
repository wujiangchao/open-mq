
package com.open.openmq.remoting.netty;

import com.open.openmq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

/**
 * Common remoting command processor
 */
public interface NettyRequestProcessor {
    RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
        throws Exception;

    boolean rejectRequest();
}
