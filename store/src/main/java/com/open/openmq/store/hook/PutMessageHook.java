package com.open.openmq.store.hook;

import com.open.openmq.common.message.MessageExt;
import com.open.openmq.store.PutMessageResult;

/**
 * @Description TODO
 * @Date 2023/3/27 10:35
 * @Author jack wu
 */
public interface PutMessageHook {

    /**
     * Name of the hook.
     *
     * @return name of the hook
     */
    String hookName();

    /**
     *  Execute before put message. For example, Message verification or special message transform
     * @param msg
     * @return
     */
    PutMessageResult executeBeforePutMessage(MessageExt msg);
}
