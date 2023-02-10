package com.open.openmq.common.message;

import java.io.Serializable;
import java.util.Map;

/**
 * @Description TODO
 * @Date 2023/2/6 22:15
 * @Author jack wu
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 8445773977080409628L;
    private String topic;
    private int flag;
    private Map<String, String> properties;
    private byte[] body;
    private String transactionId;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }
}
