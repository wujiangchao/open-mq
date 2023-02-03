package com.open.openmq.client.producer;

import com.open.openmq.client.MQAdmin;
import com.open.openmq.client.exception.MQClientException;

/**
 * @Description
 * @Date 2023/1/30 23:49
 * @Author jack wu
 */
public interface Producer extends MQAdmin {
    void start() throws MQClientException;

    void shutdown();
}
