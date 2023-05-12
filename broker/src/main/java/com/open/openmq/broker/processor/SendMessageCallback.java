

package com.open.openmq.broker.processor;

import com.open.openmq.client.hook.SendMessageContext;
import com.open.openmq.remoting.protocol.RemotingCommand;

public interface SendMessageCallback {
    /**
     * On send complete.
     *
     * @param ctx send context
     * @param response send response
     */
    void onComplete(SendMessageContext ctx, RemotingCommand response);
}
