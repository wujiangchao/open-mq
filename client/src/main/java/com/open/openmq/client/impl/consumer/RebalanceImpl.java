package com.open.openmq.client.impl.consumer;

import com.open.openmq.common.protocol.heartbeat.SubscriptionData;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @Description TODO
 * @Date 2023/3/20 16:17
 * @Author jack wu
 */
public  abstract class RebalanceImpl {

    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner =
            new ConcurrentHashMap<String, SubscriptionData>();


    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }
}
