
package com.open.openmq.client.latency;

/**
 * RocketMQ中的延迟故障机制是为了帮助Producer能够通过消息发送延迟或者消息发送结果主动感知Broker忙碌或者故障，
 * 消息发送延迟或者消息发送失败时可以将Broker排除在选择列表之外
 * <p>
 * 这个机制只有在producer发送消息时，由Producer自动选择MessageQueue才会生效，
 * 如果发送消息时指定了要发送的MessageQueue或者指定了MessageQueueSelector，
 * 即使开启了这个配置，也不会生效。
 *
 * @param <T>
 */
public interface LatencyFaultTolerance<T> {


    /**
     * 更新延迟信息
     *
     * @param name
     * @param currentLatency
     * @param notAvailableDuration
     */
    void updateFaultItem(final T name/*brokeName*/, final long currentLatency, final long notAvailableDuration);

    /**
     * 查询broker是否可用
     *
     * @param name
     * @return
     */
    boolean isAvailable(final T name/*brokeName*/);

    /**
     * 删除延迟信息
     *
     * @param name
     */
    void remove(final T name/*brokeName*/);

    /**
     * 选择一个broker
     *
     * @return
     */
    T pickOneAtLeast();
}
