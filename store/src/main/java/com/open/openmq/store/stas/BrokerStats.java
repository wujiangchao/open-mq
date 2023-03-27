package com.open.openmq.store.stas;

import com.open.openmq.store.MessageStore;

/**
 * @Description TODO
 * @Date 2023/3/24 11:26
 * @Author jack wu
 */
public class BrokerStats {
    private final MessageStore defaultMessageStore;

    public BrokerStats(MessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }
}
