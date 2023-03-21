
package com.open.openmq.common.protocol.heartbeat;

/**
 * Message model
 */
public enum MessageModel {
    /**
     * 广播
     */
    BROADCASTING("BROADCASTING"),
    /**
     * 集群
     */
    CLUSTERING("CLUSTERING");

    private String modeCN;

    MessageModel(String modeCN) {
        this.modeCN = modeCN;
    }

    public String getModeCN() {
        return modeCN;
    }
}
