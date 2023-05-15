
package com.open.openmq.client.consumer.listener;

public enum ConsumeOrderlyStatus {
    /**
     * Success consumption
     */
    SUCCESS,

    /**
     * Suspend current queue a moment
     */
    SUSPEND_CURRENT_QUEUE_A_MOMENT;
}
