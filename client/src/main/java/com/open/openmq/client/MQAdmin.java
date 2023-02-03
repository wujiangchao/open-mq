package com.open.openmq.client;

import com.open.openmq.client.exception.MQClientException;

/**
 * @Description Base interface for MQ management
 * @Date 2023/1/30 23:50
 * @Author jack wu
 */
public interface MQAdmin {

    /**
     * 获取以毫秒为单位的某个时间的消息队列偏移量
     * 由于IO开销较大，请谨慎调用
     *
     * @param mq        Instance of MessageQueue
     * @param timestamp from when in milliseconds.
     * @return offset
     */
    long searchOffset(final MessageQueue mq, final long timestamp) throws MQClientException;

    /**
     * Gets the max offset
     *
     * @param mq Instance of MessageQueue
     * @return the max offset
     */
    long maxOffset(final MessageQueue mq) throws MQClientException;

    /**
     * Gets the minimum offset
     *
     * @param mq Instance of MessageQueue
     * @return the minimum offset
     */
    long minOffset(final MessageQueue mq) throws MQClientException;


    /**
     * Query message according to message id
     *
     * @param offsetMsgId message id
     * @return message
     */
    MessageExt viewMessage(final String offsetMsgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;
}
