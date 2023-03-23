package com.open.openmq.client.impl.consumer;

import com.open.openmq.client.impl.factory.MQClientInstance;
import com.open.openmq.common.ServiceThread;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @Description TODO
 * @Date 2023/3/20 18:09
 * @Author jack wu
 */
public class PullMessageService extends ServiceThread {


    private final LinkedBlockingQueue<MessageRequest> messageRequestQueue = new LinkedBlockingQueue<MessageRequest>();

    private final MQClientInstance mQClientFactory;
    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        while (!this.isStopped()) {
            try {
                MessageRequest messageRequest = this.messageRequestQueue.take();
                if (messageRequest.getMessageRequestMode() == MessageRequestMode.POP) {
                    this.popMessage((PopRequest)messageRequest);
                } else {
                    this.pullMessage((PullRequest)messageRequest);
                }
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                log.error("Pull Message Service Run Method exception", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    private void pullMessage(final PullRequest pullRequest) {
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(pullRequest.getConsumerGroup());
        if (consumer != null) {
            DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
            impl.pullMessage(pullRequest);
        } else {
            log.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
        }
    }
}