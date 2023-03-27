
package com.open.openmq.store;

public enum GetMessageStatus {

    /**
     * 找到了消息
     */
    FOUND,

    /**
     * 没匹配到消息
     */
    NO_MATCHED_MESSAGE,

    MESSAGE_WAS_REMOVING,

    OFFSET_FOUND_NULL,

    OFFSET_OVERFLOW_BADLY,

    OFFSET_OVERFLOW_ONE,

    OFFSET_TOO_SMALL,

    NO_MATCHED_LOGIC_QUEUE,

    NO_MESSAGE_IN_QUEUE,
}
