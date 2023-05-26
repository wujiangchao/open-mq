
package com.open.openmq.client.consumer.rebalance;


import com.open.openmq.client.log.ClientLogger;
import com.open.openmq.common.message.MessageQueue;
import com.open.openmq.logging.InternalLogger;

import java.util.ArrayList;
import java.util.List;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely extends AbstractAllocateMessageQueueStrategy {

    public AllocateMessageQueueAveragely() {
        log = ClientLogger.getLog();
    }

    public AllocateMessageQueueAveragely(InternalLogger log) {
        super(log);
    }

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
                                       List<String> cidAll) {

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return result;
        }

        int index = cidAll.indexOf(currentCID);
        int mod = mqAll.size() % cidAll.size();
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
