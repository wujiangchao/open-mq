package com.open.openmq.common.protocol.route;

import com.open.openmq.common.MixAll;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * @Description Broker分为Master与Slave，一个Master可以对应多个Slave，
 * 但是一个Slave只能对应一个Master，Master与Slave的对应关系通过指定相同的BrokerName，
 * 不同的BrokerId来定义，BrokerId为0表示Master，非0表示Slave。Master也可以部署多个。
 * 每个Broker与Name Server集群中的所有节点建立长连接，定时注册Topic信息到所有Name Server
 * @Date 2023/2/18 21:02
 * @Author jack wu
 */
public class BrokerData implements Comparable<BrokerData> {
    /**
     * broker 所属集群
     */
    private String cluster;
    /**
     * broker名称
     */
    private String brokerName;

    /**
     * The container that store the all single instances for the current broker replication cluster.
     * The key is the brokerId, and the value is the address of the single broker instance.
     * 同一个brokerName下可以有一个Master和多个Slave，所以brokerAddrs是一个集合
     */
    private HashMap<Long, String> brokerAddrs;
    private String zoneName;
    private final Random random = new Random();

    /**
     * Enable acting master or not, used for old version HA adaption,
     */
    private boolean enableActingMaster = false;

    public BrokerData() {

    }

    public BrokerData(BrokerData brokerData) {
        this.cluster = brokerData.cluster;
        this.brokerName = brokerData.brokerName;
        if (brokerData.brokerAddrs != null) {
            this.brokerAddrs = new HashMap<>(brokerData.brokerAddrs);
        }
        this.enableActingMaster = brokerData.enableActingMaster;
    }

    public BrokerData(String cluster, String brokerName, HashMap<Long, String> brokerAddrs) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = brokerAddrs;
    }

    public BrokerData(String cluster, String brokerName, HashMap<Long, String> brokerAddrs, boolean enableActingMaster) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = brokerAddrs;
        this.enableActingMaster = enableActingMaster;
    }

    public BrokerData(String cluster, String brokerName, HashMap<Long, String> brokerAddrs, boolean enableActingMaster, String zoneName) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = brokerAddrs;
        this.enableActingMaster = enableActingMaster;
        this.zoneName = zoneName;
    }

    /**
     * Selects a (preferably master) broker address from the registered list. If the master's address cannot be found, a
     * slave broker address is selected in a random manner.
     *
     * @return Broker address.
     */
    public String selectBrokerAddr() {
        String masterAddress = this.brokerAddrs.get(MixAll.MASTER_ID);

        if (masterAddress == null) {
            List<String> addrs = new ArrayList<>(brokerAddrs.values());
            return addrs.get(random.nextInt(addrs.size()));
        }

        return masterAddress;
    }

    public HashMap<Long, String> getBrokerAddrs() {
        return brokerAddrs;
    }

    public void setBrokerAddrs(HashMap<Long, String> brokerAddrs) {
        this.brokerAddrs = brokerAddrs;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public boolean isEnableActingMaster() {
        return enableActingMaster;
    }

    public void setEnableActingMaster(boolean enableActingMaster) {
        this.enableActingMaster = enableActingMaster;
    }

    public String getZoneName() {
        return zoneName;
    }

    public void setZoneName(String zoneName) {
        this.zoneName = zoneName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerAddrs == null) ? 0 : brokerAddrs.hashCode());
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        BrokerData other = (BrokerData) obj;
        if (brokerAddrs == null) {
            if (other.brokerAddrs != null) {
                return false;
            }
        } else if (!brokerAddrs.equals(other.brokerAddrs)) {
            return false;
        }
        return StringUtils.equals(brokerName, other.brokerName);
    }

    @Override
    public String toString() {
        return "BrokerData [brokerName=" + brokerName + ", brokerAddrs=" + brokerAddrs + ", enableActingMaster=" + enableActingMaster + "]";
    }

    @Override
    public int compareTo(BrokerData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
