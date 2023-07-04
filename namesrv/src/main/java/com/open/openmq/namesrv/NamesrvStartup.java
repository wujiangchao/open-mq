package com.open.openmq.namesrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.open.openmq.common.ControllerConfig;
import com.open.openmq.common.MQVersion;
import com.open.openmq.common.MixAll;
import com.open.openmq.common.constant.LoggerName;
import com.open.openmq.common.namesrv.NamesrvConfig;
import com.open.openmq.logging.InternalLogger;
import com.open.openmq.logging.InternalLoggerFactory;
import com.open.openmq.remoting.netty.NettyClientConfig;
import com.open.openmq.remoting.netty.NettyServerConfig;
import com.open.openmq.remoting.protocol.RemotingCommand;
import com.open.openmq.srvutil.ServerUtil;
import com.open.openmq.srvutil.ShutdownHookThread;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.LoggerFactory;


import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * @Description TODO
 * @Date 2023/2/18 23:36
 * @Author jack wu
 */
public class NamesrvStartup {

    private static InternalLogger log;

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
        /**
         * $ bin/mqnamesrv -h
         * usage: mqnamesrv [-c <arg>] [-h] [-n <arg>] [-p]
         *  -c,--configFile <arg>    Name server config properties file
         *  -h,--help                Print help
         *  -n,--namesrvAddr <arg>   Name server address list, eg: 192.168.0.1:9876;192.168.0.2:9876
         *  -p,--printConfigItem     Print all config item
         *
         *  主要可以通过-c指定一个配置文件，而该配置文件中可配置的内容可以通过mqnamesrv -p打印出来，
         *  它是通过日志输出的方式打印出来的
         */
        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(file)));
                properties = new Properties();
                properties.load(in);
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);
                MixAll.properties2Object(properties, nettyClientConfig);
                MixAll.properties2Object(properties, controllerConfig);

                namesrvConfig.setConfigStorePath(file);

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }

        if (commandLine.hasOption('p')) {
            MixAll.printObjectProperties(null, namesrvConfig);
            MixAll.printObjectProperties(null, nettyServerConfig);
            MixAll.printObjectProperties(null, nettyClientConfig);
            MixAll.printObjectProperties(null, controllerConfig);
            System.exit(0);
        }

        //1、commandLine2Properties   2、properties2Object
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

        if (null == namesrvConfig.getOpenmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the OpenMQ installation%n", MixAll.OPENMQ_HOME_ENV);
            System.exit(-2);
        }

        /**
         * 初始化Logback日志工厂
         * 默认使用Logback作为日志输出
         */
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(namesrvConfig.getOpenmqHome() + "/conf/logback_namesrv.xml");

        log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);
    }

    public static void controllerManagerMain() {
        try {
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                //createAndStartControllerManager();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

//    public static void createAndStartControllerManager() throws Exception {
//        ControllerManager controllerManager = createControllerManager();
//        start(controllerManager);
//        String tip = "The ControllerManager boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
//        log.info(tip);
//        System.out.printf("%s%n", tip);
//    }

//    public static ControllerManager createControllerManager() throws Exception {
//        NettyServerConfig controllerNettyServerConfig = (NettyServerConfig) nettyServerConfig.clone();
//        ControllerManager controllerManager = new ControllerManager(controllerConfig, controllerNettyServerConfig, nettyClientConfig);
//        // remember all configs to prevent discard
//        controllerManager.getConfiguration().registerConfig(properties);
//        return controllerManager;
//    }

    public static void createAndStartNamesrvController() throws Exception {
        NamesrvController controller = createNamesrvController();
        start(controller);
        String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
        log.info(tip);
        System.out.printf("%s%n", tip);
    }

    public static NamesrvController createNamesrvController() {
        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig, nettyClientConfig);
        // remember all configs to prevent discard
        controller.getConfiguration().registerConfig(properties);
        return controller;
    }

    public static NamesrvController start(final NamesrvController controller) throws Exception {

        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }

        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }

        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controller.shutdown();
            return null;
        }));

        controller.start();

        return controller;
    }

//    public static ControllerManager start(final ControllerManager controllerManager) throws Exception {
//        if (null == controllerManager) {
//            throw new IllegalArgumentException("ControllerManager is null");
//        }
//
//        boolean initResult = controllerManager.initialize();
//        if (!initResult) {
//            controllerManager.shutdown();
//            System.exit(-3);
//        }
//
//        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
//            controllerManager.shutdown();
//            return null;
//        }));
//
//        controllerManager.start();
//
//        return controllerManager;
//    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config items");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

}
