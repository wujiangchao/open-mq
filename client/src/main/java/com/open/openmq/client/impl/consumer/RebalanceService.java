package com.open.openmq.client.impl.consumer;

import com.open.openmq.client.impl.factory.MQClientInstance;
import com.open.openmq.client.log.ClientLogger;
import com.open.openmq.common.ServiceThread;
import com.open.openmq.logging.InternalLogger;

/**
 * @Description Rebalance指的是将⼀个Topic下的多个Queue在同⼀个ConsumerGroup中
 * 的多个 Consumer间进行重新分配的过程，它能够提升消息的并行消费能力
 * @Date 2023/3/21 21:24
 * @Author jack wu
 */
public class RebalanceService extends ServiceThread {
    private static long waitInterval =
            Long.parseLong(System.getProperty(
                    "rocketmq.client.rebalance.waitInterval", "20000"));
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mqClientFactory;

    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            //RebalanceService线程默认每隔20s执行一次mqClientFactory.doRebalance()方法
            this.waitForRunning(waitInterval);
            this.mqClientFactory.doRebalance();
        }

        log.info(this.getServiceName() + " service end");
    }
}
