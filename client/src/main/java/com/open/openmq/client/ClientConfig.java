package com.open.openmq.client;

import com.open.openmq.common.UtilAll;
import com.open.openmq.common.protocol.NamespaceUtil;
import com.open.openmq.common.utils.NameServerAddressUtils;
import com.open.openmq.remoting.common.RemotingUtil;
import com.open.openmq.remoting.protocol.RequestType;

/**
 * @Description TODO
 * @Date 2023/2/1 22:54
 * @Author jack wu
 */
public class ClientConfig {
    public static final String SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY = "com.rocketmq.sendMessageWithVIPChannel";
    public static final String DECODE_READ_BODY = "com.rocketmq.read.body";
    public static final String DECODE_DECOMPRESS_BODY = "com.rocketmq.decompress.body";
    protected String namespace;
    private String namesrvAddr = NameServerAddressUtils.getNameServerAddresses();

    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();

    private String unitName;

    private String clientIP = RemotingUtil.getLocalAddress();

    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");

    private boolean useTLS;

    private int mqClientApiTimeout = 3 * 1000;


    /**
     * 当isUnitMode设置为true时，RocketMQ将进入单元测试模式。在这种模式下，RocketMQ将不会启动NameServer进程，
     * 而是直接使用本地文件系统作为消息存储。这种模式主要用于快速开发和测试，可以方便地模拟生产环境中的消息发送和消费。
     * 当isUnitMode设置为false时，RocketMQ将进入正常模式。在这种模式下，RocketMQ将启动NameServer进程，并使用
     * 分布式文件系统作为消息存储。这种模式主要用于生产环境中的消息发送和消费。
     * 需要注意的是，在单元测试模式下，由于没有NameServer进程，因此无法进行集群部署和分布式消息处理。因此，在开发过程中，
     * 需要根据实际需求选择合适的运行模式。
     */
    private boolean unitMode = false;


    /**
     * Enable stream request type will inject a RPCHook to add corresponding request type to remoting layer.
     * And it will also generate a different client id to prevent unexpected reuses of MQClientInstance.
     */
    protected boolean enableStreamRequestType = false;

    /**
     * Pulling topic information interval from the named server
     */
    private int pollNameServerInterval = 1000 * 30;
    /**
     * Heartbeat interval in microseconds with message broker
     */
    private int heartbeatBrokerInterval = 1000 * 30;

    /**
     * Offset persistent interval for consumer
     */
    private int persistConsumerOffsetInterval = 1000 * 5;

    private boolean decodeReadBody = Boolean.parseBoolean(System.getProperty(DECODE_READ_BODY, "true"));
    private boolean decodeDecompressBody = Boolean.parseBoolean(System.getProperty(DECODE_DECOMPRESS_BODY, "true"));
    private boolean vipChannelEnabled = Boolean.parseBoolean(System.getProperty(SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "false"));

    public String buildMQClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClientIP());

        sb.append("@");
        sb.append(this.getInstanceName());
        if (!UtilAll.isBlank(this.unitName)) {
            sb.append("@");
            sb.append(this.unitName);
        }

        if (enableStreamRequestType) {
            sb.append("@");
            sb.append(RequestType.STREAM);
        }

        return sb.toString();
    }

    public ClientConfig cloneClientConfig() {
        ClientConfig cc = new ClientConfig();
        cc.namesrvAddr = namesrvAddr;
        cc.clientIP = clientIP;
        cc.instanceName = instanceName;
        cc.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
//        cc.pollNameServerInterval = pollNameServerInterval;
//        cc.heartbeatBrokerInterval = heartbeatBrokerInterval;
//        cc.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
//        cc.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
        cc.unitMode = unitMode;
        cc.unitName = unitName;
//        cc.vipChannelEnabled = vipChannelEnabled;
        cc.useTLS = useTLS;
        cc.namespace = namespace;
//        cc.language = language;
//        cc.mqClientApiTimeout = mqClientApiTimeout;
//        cc.decodeReadBody = decodeReadBody;
//        cc.decodeDecompressBody = decodeDecompressBody;
        cc.enableStreamRequestType = enableStreamRequestType;
        return cc;
    }

    public void resetClientConfig(final ClientConfig cc) {
        this.namesrvAddr = cc.namesrvAddr;
        this.clientIP = cc.clientIP;
        this.instanceName = cc.instanceName;
        this.clientCallbackExecutorThreads = cc.clientCallbackExecutorThreads;
//        this.pollNameServerInterval = cc.pollNameServerInterval;
//        this.heartbeatBrokerInterval = cc.heartbeatBrokerInterval;
//        this.persistConsumerOffsetInterval = cc.persistConsumerOffsetInterval;
//        this.pullTimeDelayMillsWhenException = cc.pullTimeDelayMillsWhenException;
        this.unitMode = cc.unitMode;
        this.unitName = cc.unitName;
//        this.vipChannelEnabled = cc.vipChannelEnabled;
        this.useTLS = cc.useTLS;
        this.namespace = cc.namespace;
//        this.language = cc.language;
//        this.mqClientApiTimeout = cc.mqClientApiTimeout;
//        this.decodeReadBody = cc.decodeReadBody;
//        this.decodeDecompressBody = cc.decodeDecompressBody;
        this.enableStreamRequestType = cc.enableStreamRequestType;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String withNamespace(String resource) {
        return NamespaceUtil.wrapNamespace(this.getNamespace(), resource);
    }

    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }

    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }

    public boolean isEnableStreamRequestType() {
        return enableStreamRequestType;
    }

    public void setEnableStreamRequestType(boolean enableStreamRequestType) {
        this.enableStreamRequestType = enableStreamRequestType;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public String getClientIP() {
        return clientIP;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public boolean isUseTLS() {
        return useTLS;
    }

    public void setUseTLS(boolean useTLS) {
        this.useTLS = useTLS;
    }

    public int getMqClientApiTimeout() {
        return mqClientApiTimeout;
    }

    public void setMqClientApiTimeout(int mqClientApiTimeout) {
        this.mqClientApiTimeout = mqClientApiTimeout;
    }

    public int getPollNameServerInterval() {
        return pollNameServerInterval;
    }

    public void setPollNameServerInterval(int pollNameServerInterval) {
        this.pollNameServerInterval = pollNameServerInterval;
    }

    public int getHeartbeatBrokerInterval() {
        return heartbeatBrokerInterval;
    }

    public void setHeartbeatBrokerInterval(int heartbeatBrokerInterval) {
        this.heartbeatBrokerInterval = heartbeatBrokerInterval;
    }

    public int getPersistConsumerOffsetInterval() {
        return persistConsumerOffsetInterval;
    }

    public void setPersistConsumerOffsetInterval(int persistConsumerOffsetInterval) {
        this.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
    }

    public boolean isDecodeReadBody() {
        return decodeReadBody;
    }

    public void setDecodeReadBody(boolean decodeReadBody) {
        this.decodeReadBody = decodeReadBody;
    }

    public boolean isDecodeDecompressBody() {
        return decodeDecompressBody;
    }

    public void setDecodeDecompressBody(boolean decodeDecompressBody) {
        this.decodeDecompressBody = decodeDecompressBody;
    }

    public boolean isVipChannelEnabled() {
        return vipChannelEnabled;
    }

    public void setVipChannelEnabled(boolean vipChannelEnabled) {
        this.vipChannelEnabled = vipChannelEnabled;
    }
}
