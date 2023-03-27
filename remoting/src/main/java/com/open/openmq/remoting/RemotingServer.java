package com.open.openmq.remoting;

import com.open.openmq.remoting.netty.NettyRequestProcessor;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 * @Description TODO
 * @Date 2023/3/24 11:56
 * @Author jack wu
 */
public interface RemotingServer extends RemotingService{
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
                           final ExecutorService executor);

    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);
}
