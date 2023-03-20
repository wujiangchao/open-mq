package com.open.openmq.common.protocol.heartbeat;

import com.open.openmq.common.filter.ExpressionType;

/**
 * @Description TODO
 * @Date 2023/3/20 16:20
 * @Author jack wu
 */
public class SubscriptionData {
    public final static String SUB_ALL = "*";
    private long subVersion = System.currentTimeMillis();
    private String expressionType = ExpressionType.TAG;
    private String topic;

    public SubscriptionData() {

    }


    public long getSubVersion() {
        return subVersion;
    }

    public void setSubVersion(long subVersion) {
        this.subVersion = subVersion;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public void setExpressionType(String expressionType) {
        this.expressionType = expressionType;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
