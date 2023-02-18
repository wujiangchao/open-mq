package com.open.openmq.client.latency;

import com.open.openmq.client.impl.producer.TopicPublishInfo;
import com.open.openmq.common.message.MessageQueue;

/**
 * @Description queue分布在各个不同的broker服务器中时，当尝试向其中一个queue发送消息时，
 * 如果出现耗时过长或者发送失败的情况，RocketMQ则会尝试重试发送。同样的消息第一次发送失败或耗时过长，
 * 可能是网络波动或者相关broker停止导致，如果短时间再次重试极有可能还是同样的情况。
 * MQ为我们提供了延迟故障自动切换queue的功能，并且会根据故障次数和失败等级来预判故障时间并自动恢复
 * @Date 2023/2/14 15:00
 * @Author jack wu
 */
public class MQFaultStrategy {


    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        if (this.sendLatencyFaultEnable) {
            try {
                int index = tpInfo.getSendWhichQueue().incrementAndGet();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0) {
                        pos = 0;
                    }
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        return mq;
                    }
                }

                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }

        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }
}
