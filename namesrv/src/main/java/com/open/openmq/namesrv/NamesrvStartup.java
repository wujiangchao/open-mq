package com.open.openmq.namesrv;

import com.open.openmq.common.ControllerConfig;
import com.open.openmq.common.namesrv.NamesrvConfig;
import com.open.openmq.remoting.netty.NettyClientConfig;
import com.open.openmq.remoting.netty.NettyServerConfig;
import sun.tools.jar.CommandLine;

import java.util.Properties;

/**
 * @Description TODO
 * @Date 2023/2/18 23:36
 * @Author jack wu
 */
public class NamesrvStartup {
    /**
     * 用于保存、读取配置文件
     * 临时存储全部的配置k-v，包含-c指定的启动文件和-p启动的变量
     */
    private static Properties properties = null;
    /**
     * 从properties中解析出来的全部namesrv配置
     */
    private static NamesrvConfig namesrvConfig = null;
    /**
     * 从properties中解析出来的全部Controller和Namesrv RPC服务端启动配置。
     * 特别注意Controller的配置是clone出来的，和Namesrv使用的不是同一个对象。
     */
    private static NettyServerConfig nettyServerConfig = null;
    /**
     * 从properties中解析出来的全部Controller和Namesrv RPC客户端启动配置
     */
    private static NettyClientConfig nettyClientConfig = null;
    /**
     * 从properties中解析出来的全部Controller需要的启动配置
     */
    private static ControllerConfig controllerConfig = null;

    public static void main(String[] args) {
        main0(args);
        //主要是判断当前Namesrv是否配置允许内嵌启动一个Controller实例。
        controllerManagerMain();
    }

    public static void main0(String[] args) {
        try {
            parseCommandlineAndConfigFile(args);
            createAndStartNamesrvController();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static void parseCommandlineAndConfigFile(String[] args) throws Exception {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
            return;
        }
        namesrvConfig = new NamesrvConfig();
        nettyServerConfig = new NettyServerConfig();
        nettyClientConfig = new NettyClientConfig();
        nettyServerConfig.setListenPort(9876);
        controllerConfig = new ControllerConfig();


    }

    public static void controllerManagerMain() {
        try {
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                createAndStartControllerManager();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

}
