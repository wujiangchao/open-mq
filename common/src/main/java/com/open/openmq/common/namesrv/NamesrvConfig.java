package com.open.openmq.common.namesrv;

import com.open.openmq.common.MixAll;

import java.io.File;

/**
 * @Description 从properties中解析出来的全部namesrv配置
 * @Date 2023/2/20 16:10
 * @Author jack wu
 */
public class NamesrvConfig {
    /**
     * RocketMQ安装目录
     * RocketMQ home 目录如果没有指定的话，默认值为系统环境变量ROCKETMQ_HOME
     * 通过System.getenv获取，可以在~/.profile中export,或者可以在配置文件中指定rocketmqHome=***
     */
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    /**
     * KvConfig的配置文件路径
     */
    private String kvConfigPath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "kvConfig.json";
    /**
     * NamesrvConfig(以及NettyServerConfig)的配置文件路径
     * 默认为System.getProperty("user.home")/namesrv/namesrv.properties
     */
    private String configStorePath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "namesrv.properties";
    private String productEnvName = "center";
    /**
     * 决定NamesrvController#registerProcessor()注册处理器时
     * 用的是ClusterTestRequestProcessor 还是 DefaultRequestProcessor
     */
    private boolean clusterTest = false;
    private boolean orderMessageEnable = false;
    private boolean returnOrderTopicConfigToBroker = true;

    /**
     * Is startup the controller in this name-srv
     */
    private boolean enableControllerInNamesrv = false;


    public boolean isEnableControllerInNamesrv() {
        return enableControllerInNamesrv;
    }

    public String getConfigStorePath() {
        return configStorePath;
    }

    public void setConfigStorePath(String configStorePath) {
        this.configStorePath = configStorePath;
    }

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }
}
