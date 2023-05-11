

package com.open.openmq.client.producer;


import com.open.openmq.common.message.Message;

public interface RequestCallback {
    void onSuccess(final Message message);

    void onException(final Throwable e);
}
