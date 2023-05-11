
package com.open.openmq.client.producer;


import com.open.openmq.common.message.Message;
import com.open.openmq.common.message.MessageQueue;

import java.util.List;

public interface MessageQueueSelector {
    MessageQueue select(final List<MessageQueue> mqs, final Message msg, final Object arg);
}
