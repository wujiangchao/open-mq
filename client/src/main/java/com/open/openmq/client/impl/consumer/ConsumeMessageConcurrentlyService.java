package com.open.openmq.client.impl.consumer;

import com.open.openmq.client.consumer.DefaultMQPushConsumer;
import com.open.openmq.client.consumer.listener.MessageListenerConcurrently;
import com.open.openmq.client.log.ClientLogger;
import com.open.openmq.common.ThreadFactoryImpl;
import com.open.openmq.common.message.MessageExt;
import com.open.openmq.common.message.MessageQueue;
import com.open.openmq.common.protocol.body.ConsumeMessageDirectlyResult;
import com.open.openmq.logging.InternalLogger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Description ConsumeMessageConcurrentlyService是一个服务类，用于实现并发消息消费。并发消息消费是一种高效的消息消费模式，
 * 它能够充分利用系统资源，提高消息处理的吞吐量。
 * 具体来说，ConsumeMessageConcurrentlyService实现了以下功能：
 * 1、 实现了MessageListenerConcurrently接口，指定了消息消费的逻辑。
 * 2、使用Select算法将多个消费者线程分配到多个消息队列上，实现负载均衡。
 * 3、每个消费者线程独立处理消息，可以同时处理多个消息，提高处理效率。
 * 4、当有新的消息到达时，ConsumeMessageConcurrentlyService会将其分配给空闲的消费者线程，以充分利用系统资源。
 * 通过使用ConsumeMessageConcurrentlyService，您可以实现并发消息消费，提高消息处理的吞吐量。
 * @Date 2023/6/19 17:25
 * @Author jack wu
 */
public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {

    private static final InternalLogger log = ClientLogger.getLog();
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerConcurrently messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;

    private final ScheduledExecutorService scheduledExecutorService;
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
                                             MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        String consumeThreadPrefix = null;
        if (consumerGroup.length() > 100) {
            consumeThreadPrefix = new StringBuilder("ConsumeMessageThread_").append(consumerGroup, 0, 100).append("_").toString();
        } else {
            consumeThreadPrefix = new StringBuilder("ConsumeMessageThread_").append(consumerGroup).append("_").toString();
        }
        this.consumeExecutor = new ThreadPoolExecutor(
                this.defaultMQPushConsumer.getConsumeThreadMin(),
                this.defaultMQPushConsumer.getConsumeThreadMax(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.consumeRequestQueue,
                new ThreadFactoryImpl(consumeThreadPrefix));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));
    }

    @Override
    public void start() {
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    cleanExpireMsg();
                } catch (Throwable e) {
                    log.error("scheduleAtFixedRate cleanExpireMsg exception", e);
                }
            }

        }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
    }

    private void cleanExpireMsg() {
        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it =
                this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue pq = next.getValue();
            pq.cleanExpiredMsg(this.defaultMQPushConsumer);
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
}
