package com.open.openmq.namesrv;

import com.open.openmq.common.namesrv.NamesrvConfig;
import com.open.openmq.remoting.netty.NettyClientConfig;
import com.open.openmq.remoting.netty.NettyServerConfig;

/**
 * @Description nameserver主要控制类
 * @Date 2023/2/21 22:31
 * @Author jack wu
 */
public class NamesrvController {

    private final NamesrvConfig namesrvConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;

    private final KVConfigManager kvConfigManager;
    private final RouteInfoManager routeInfoManager;
}
