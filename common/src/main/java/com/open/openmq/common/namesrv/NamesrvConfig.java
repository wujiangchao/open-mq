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
     * Indicates the nums of thread to handle client requests, like GET_ROUTEINTO_BY_TOPIC.
     */
    private int clientRequestThreadPoolNums = 8;
    /**
     * Indicates the nums of thread to handle broker or operation requests, like REGISTER_BROKER.
     */
    private int defaultThreadPoolNums = 16;
    /**
     * Indicates the capacity of queue to hold client requests.
     */
    private int clientRequestThreadPoolQueueCapacity = 50000;
    /**
     * Indicates the capacity of queue to hold broker or operation requests.
     */
    private int defaultThreadPoolQueueCapacity = 10000;

    /**
     * Is startup the controller in this name-srv
     */
    private boolean enableControllerInNamesrv = false;

    /**
     * Interval of periodic scanning for non-active broker;
     */
    private long scanNotActiveBrokerInterval = 5 * 1000;


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

    public int getClientRequestThreadPoolNums() {
        return clientRequestThreadPoolNums;
    }

    public void setClientRequestThreadPoolNums(int clientRequestThreadPoolNums) {
        this.clientRequestThreadPoolNums = clientRequestThreadPoolNums;
    }

    public int getDefaultThreadPoolNums() {
        return defaultThreadPoolNums;
    }

    public void setDefaultThreadPoolNums(int defaultThreadPoolNums) {
        this.defaultThreadPoolNums = defaultThreadPoolNums;
    }

    public int getClientRequestThreadPoolQueueCapacity() {
        return clientRequestThreadPoolQueueCapacity;
    }

    public void setClientRequestThreadPoolQueueCapacity(int clientRequestThreadPoolQueueCapacity) {
        this.clientRequestThreadPoolQueueCapacity = clientRequestThreadPoolQueueCapacity;
    }

    public int getDefaultThreadPoolQueueCapacity() {
        return defaultThreadPoolQueueCapacity;
    }

    public void setDefaultThreadPoolQueueCapacity(int defaultThreadPoolQueueCapacity) {
        this.defaultThreadPoolQueueCapacity = defaultThreadPoolQueueCapacity;
    }

    public boolean isClusterTest() {
        return clusterTest;
    }

    public void setClusterTest(boolean clusterTest) {
        this.clusterTest = clusterTest;
    }

    public long getScanNotActiveBrokerInterval() {
        return scanNotActiveBrokerInterval;
    }

    public void setScanNotActiveBrokerInterval(long scanNotActiveBrokerInterval) {
        this.scanNotActiveBrokerInterval = scanNotActiveBrokerInterval;
    }

    public String getProductEnvName() {
        return productEnvName;
    }

    public void setProductEnvName(String productEnvName) {
        this.productEnvName = productEnvName;
    }

    public boolean isOrderMessageEnable() {
        return orderMessageEnable;
    }

    public void setOrderMessageEnable(boolean orderMessageEnable) {
        this.orderMessageEnable = orderMessageEnable;
    }

    public String getKvConfigPath() {
        return kvConfigPath;
    }

    public void setKvConfigPath(String kvConfigPath) {
        this.kvConfigPath = kvConfigPath;
    }
}
