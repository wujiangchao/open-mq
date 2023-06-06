package com.open.openmq.common;

import java.io.File;

/**
 * @Description TODO
 * @Date 2023/2/20 17:16
 * @Author jack wu
 */
public class ControllerConfig {

    private String openmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    private String configStorePath = System.getProperty("user.home") + File.separator + "controller" + File.separator + "controller.properties";

    /**
     * Interval of periodic scanning for non-active broker;
     */
    private long scanNotActiveBrokerInterval = 5 * 1000;

    /**
     * Indicates the nums of thread to handle broker or operation requests, like REGISTER_BROKER.
     */
    private int controllerThreadPoolNums = 16;

    /**
     * Indicates the capacity of queue to hold client requests.
     */
    private int controllerRequestThreadPoolQueueCapacity = 50000;

    /**
     * DLedger Raft Group 的名字，同一个 DLedger Raft Group 保持一致即可。
     */
    private String controllerDLegerGroup;

    /**
     * DLedger Group 内各节点的端口信息，同一个 Group 内的各个节点配置必须要保证一致。
     */
    private String controllerDLegerPeers;

    /**
     * 节点 id，必须属于 controllerDLegerPeers 中的一个；同 Group 内各个节点要唯一。
     */
    private String controllerDLegerSelfId;

    /**
     * Whether notify broker when its role changed
     */
    private volatile boolean notifyBrokerRoleChanged = true;

    public int getControllerThreadPoolNums() {
        return controllerThreadPoolNums;
    }

    public void setControllerThreadPoolNums(int controllerThreadPoolNums) {
        this.controllerThreadPoolNums = controllerThreadPoolNums;
    }

    public int getControllerRequestThreadPoolQueueCapacity() {
        return controllerRequestThreadPoolQueueCapacity;
    }

    public void setControllerRequestThreadPoolQueueCapacity(int controllerRequestThreadPoolQueueCapacity) {
        this.controllerRequestThreadPoolQueueCapacity = controllerRequestThreadPoolQueueCapacity;
    }

    public String getControllerDLegerGroup() {
        return controllerDLegerGroup;
    }

    public void setControllerDLegerGroup(String controllerDLegerGroup) {
        this.controllerDLegerGroup = controllerDLegerGroup;
    }

    public String getControllerDLegerPeers() {
        return controllerDLegerPeers;
    }

    public void setControllerDLegerPeers(String controllerDLegerPeers) {
        this.controllerDLegerPeers = controllerDLegerPeers;
    }

    public String getControllerDLegerSelfId() {
        return controllerDLegerSelfId;
    }

    public void setControllerDLegerSelfId(String controllerDLegerSelfId) {
        this.controllerDLegerSelfId = controllerDLegerSelfId;
    }

    public boolean isNotifyBrokerRoleChanged() {
        return notifyBrokerRoleChanged;
    }

    public void setNotifyBrokerRoleChanged(boolean notifyBrokerRoleChanged) {
        this.notifyBrokerRoleChanged = notifyBrokerRoleChanged;
    }
}
