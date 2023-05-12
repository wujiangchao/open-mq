package com.open.openmq.broker.processor;

import com.open.openmq.broker.BrokerController;
import com.open.openmq.remoting.netty.NettyRequestProcessor;

/**
 * @Description TODO
 * @Date 2023/3/22 14:47
 * @Author jack wu
 */
public abstract class AbstractSendMessageProcessor implements NettyRequestProcessor {

    protected final BrokerController brokerController;

    private List<SendMessageHook> sendMessageHookList;


    protected AbstractSendMessageProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public boolean hasSendMessageHook() {
        return sendMessageHookList != null && !this.sendMessageHookList.isEmpty();
    }


    @Override
    public boolean rejectRequest() {
        return false;
    }
}
