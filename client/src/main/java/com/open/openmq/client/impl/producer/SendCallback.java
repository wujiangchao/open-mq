
package com.open.openmq.client.impl.producer;

import com.open.openmq.client.producer.SendResult;

public interface SendCallback {
    void onSuccess(final SendResult sendResult);

    void onException(final Throwable e);
}
