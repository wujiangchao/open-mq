
package com.open.openmq.client.consumer.listener;

/**
 * @author 34246
 */

public enum ConsumeReturnType {
    /**
     * consume return success
     */
    SUCCESS,
    /**
     * consume timeout ,even if success
     */
    TIME_OUT,
    /**
     * consume throw exception
     */
    EXCEPTION,
    /**
     * consume return null
     */
    RETURNNULL,
    /**
     * consume return failed
     */
    FAILED
}
