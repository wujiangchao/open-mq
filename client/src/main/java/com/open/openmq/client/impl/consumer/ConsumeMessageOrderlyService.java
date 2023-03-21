package com.open.openmq.client.impl.consumer;

import com.open.openmq.client.consumer.DefaultMQPushConsumer;
import com.open.openmq.common.message.MessageQueue;
import com.open.openmq.common.protocol.heartbeat.MessageModel;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Description TODO
 * @Date 2023/3/21 9:33
 * @Author jack wu
 */
public class ConsumeMessageOrderlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY =
            Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerOrderly messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean stopped = false;
    @Override
    public void start() {
        //如果是集群模式
        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())) {
            //启动一个定时任务，启动后1s执行，后续每20s执行一次
            //尝试对所有分配给当前consumer的队列请求broker端的消息队列锁，保证同时只有一个消费端可以消费。
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        ConsumeMessageOrderlyService.this.lockMQPeriodically();
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate lockMQPeriodically exception", e);
                    }
                }
            }, 1000 * 1, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    public synchronized void lockMQPeriodically() {
        if (!this.stopped) {
            //锁定所有消息队列
            this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
        }
    }

    @Override
    public void shutdown(long awaitTerminateMillis) {

    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {

    }

    @Override
    public void incCorePoolSize() {

    }

    @Override
    public void decCorePoolSize() {

    }

    @Override
    public int getCorePoolSize() {
        return 0;
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        return null;
    }

    @Override
    public void submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue, boolean dispathToConsume) {

    }

    @Override
    public void submitPopConsumeRequest(List<MessageExt> msgs, PopProcessQueue processQueue, MessageQueue messageQueue) {

    }
}
