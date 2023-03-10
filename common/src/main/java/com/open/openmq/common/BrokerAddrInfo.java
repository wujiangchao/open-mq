
package com.open.openmq.common;

public class BrokerAddrInfo {
    private final String clusterName;
    private final String brokerAddr;

    private int hash;

    public BrokerAddrInfo(String clusterName, String brokerAddr) {
        this.clusterName = clusterName;
        this.brokerAddr = brokerAddr;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public boolean isEmpty() {
        return clusterName.isEmpty() && brokerAddr.isEmpty();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (obj instanceof BrokerAddrInfo) {
            BrokerAddrInfo addr = (BrokerAddrInfo) obj;
            return clusterName.equals(addr.clusterName) && brokerAddr.equals(addr.brokerAddr);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0 && clusterName.length() + brokerAddr.length() > 0) {
            for (int i = 0; i < clusterName.length(); i++) {
                h = 31 * h + clusterName.charAt(i);
            }
            h = 31 * h + '_';
            for (int i = 0; i < brokerAddr.length(); i++) {
                h = 31 * h + brokerAddr.charAt(i);
            }
            hash = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return "BrokerAddrInfo [clusterName=" + clusterName + ", brokerAddr=" + brokerAddr + "]";
    }
}
