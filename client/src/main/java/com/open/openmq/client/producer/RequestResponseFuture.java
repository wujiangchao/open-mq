package com.open.openmq.client.producer;

import com.open.openmq.common.message.Message;

import java.util.concurrent.CountDownLatch;

/**
 * @Description TODO
 * @Date 2023/6/7 14:08
 * @Author jack wu
 */
public class RequestResponseFuture {

    private final String correlationId;
    private final RequestCallback requestCallback;
    private final long beginTimestamp = System.currentTimeMillis();
    private final Message requestMsg = null;
    private long timeoutMillis;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private volatile Message responseMsg = null;
    private volatile boolean sendRequestOk = true;
    private volatile Throwable cause = null;

    public RequestResponseFuture(String correlationId, long timeoutMillis, RequestCallback requestCallback) {
        this.correlationId = correlationId;
        this.timeoutMillis = timeoutMillis;
        this.requestCallback = requestCallback;
    }


    public void executeRequestCallback() {
        if (requestCallback != null) {
            if (sendRequestOk && cause == null) {
                requestCallback.onSuccess(responseMsg);
            } else {
                requestCallback.onException(cause);
            }
        }
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    public String getCorrelationId() {
        return correlationId;
    }
}
