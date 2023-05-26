

package com.open.openmq.client.consumer.rebalance;

import com.open.openmq.client.consumer.AllocateMessageQueueStrategy;
import com.open.openmq.client.log.ClientLogger;
import com.open.openmq.common.message.MessageQueue;
import com.open.openmq.logging.InternalLogger;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;


import java.util.List;

public abstract class AbstractAllocateMessageQueueStrategy implements AllocateMessageQueueStrategy {

    protected InternalLogger log;

    AbstractAllocateMessageQueueStrategy() {
        this.log = ClientLogger.getLog();
    }

    public AbstractAllocateMessageQueueStrategy(InternalLogger log) {
        this.log = log;
    }

    public boolean check(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        if (StringUtils.isEmpty(currentCID)) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (CollectionUtils.isEmpty(mqAll)) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (CollectionUtils.isEmpty(cidAll)) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return false;
        }

        return true;
    }
}
