package com.open.openmq.client.impl.producer;

import com.open.openmq.client.common.ThreadLocalIndex;
import com.open.openmq.common.message.MessageQueue;
import com.open.openmq.common.protocol.route.TopicRouteData;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description TODO
 * @Date 2023/2/11 4:10
 * @Author jack wu
 */
public class TopicPublishInfo {
    /**
     * 是否有序
     */
    private boolean orderTopic = false;
    /**
     * 是否有topic路由信息
     */
    private boolean haveTopicRouterInfo = false;
    /**
     * 消息队列  当前 topic 的 MessageQueue
     */
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();
    /**
     * 消息队列索引
     * 提供一个线程安全的Integer随机数获取服务
     * 每选择一次消息队列，该值会自增1，如果Integer.MAX_VALUE，则重置为0，用于选择消息队列
     */
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    /**
     * 路由元数据
     */
    private TopicRouteData topicRouteData;

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }

    /**
     * 通过BrokerName指定消息要发送的 Broker 服务器，
     * 即获取 BrokerName 指定的服务器上的随机一个 MessageQueue
     *
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        if (lastBrokerName == null) {
            return selectOneMessageQueue();
        } else {
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                int index = this.sendWhichQueue.incrementAndGet();
                int pos = Math.abs(index) % this.messageQueueList.size();
                // int 的最大值是(2^31) -1，而最小值是-2^31，所以按照 abs 的逻辑，
                // 如果a是最小值，则最小值前面加个负数就变成了 2^31，而 int 所能表示的最大值是 (2^31) -1，
                // 这比最大值还大了个 1，导致向上溢出，所以此时得到的结果还是最小值。
                if (pos < 0) {
                    pos = 0;
                }
                MessageQueue mq = this.messageQueueList.get(pos);
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
            return selectOneMessageQueue();
        }
    }

    /**
     * 没有指定BrokerName，随机获取一个 MessageQueue
     *
     * @return
     */
    public MessageQueue selectOneMessageQueue() {
        //获取随机数
        int index = this.sendWhichQueue.incrementAndGet();
        //获取不越界的随机数
        int pos = Math.abs(index) % this.messageQueueList.size();
        if (pos < 0) {
            pos = 0;
        }
        //随机获取一个 MessageQueue
        return this.messageQueueList.get(pos);
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
                + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }
}
