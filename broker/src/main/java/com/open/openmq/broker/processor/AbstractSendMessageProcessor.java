package com.open.openmq.broker.processor;

import com.open.openmq.broker.BrokerController;
import com.open.openmq.broker.mqtrace.AbortProcessException;
import com.open.openmq.broker.mqtrace.SendMessageContext;
import com.open.openmq.broker.mqtrace.SendMessageHook;
import com.open.openmq.common.protocol.NamespaceUtil;
import com.open.openmq.common.protocol.header.SendMessageRequestHeader;
import com.open.openmq.common.protocol.header.SendMessageResponseHeader;
import com.open.openmq.remoting.common.RemotingHelper;
import com.open.openmq.remoting.exception.RemotingCommandException;
import com.open.openmq.remoting.netty.NettyRequestProcessor;
import com.open.openmq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

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

    protected SendMessageRequestHeader parseRequestHeader(RemotingCommand request) throws RemotingCommandException {
        return SendMessageRequestHeader.parseRequestHeader(request);
    }

    public void executeSendMessageHookAfter(final RemotingCommand response, final SendMessageContext context) {
        if (hasSendMessageHook()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    if (response != null) {
                        final SendMessageResponseHeader responseHeader =
                                (SendMessageResponseHeader) response.readCustomHeader();
                        context.setMsgId(responseHeader.getMsgId());
                        context.setQueueId(responseHeader.getQueueId());
                        context.setQueueOffset(responseHeader.getQueueOffset());
                        context.setCode(response.getCode());
                        context.setErrorMsg(response.getRemark());
                    }
                    hook.sendMessageAfter(context);
                } catch (Throwable e) {
                    //ignore
                }
            }
        }
    }

    public void executeSendMessageHookBefore(final ChannelHandlerContext ctx, final RemotingCommand request,
                                             SendMessageContext context) {
        if (hasSendMessageHook()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    final SendMessageRequestHeader requestHeader = parseRequestHeader(request);

                    String namespace = NamespaceUtil.getNamespaceFromResource(requestHeader.getTopic());
                    if (null != requestHeader) {
                        context.setNamespace(namespace);
                        context.setProducerGroup(requestHeader.getProducerGroup());
                        context.setTopic(requestHeader.getTopic());
                        context.setBodyLength(request.getBody().length);
                        context.setMsgProps(requestHeader.getProperties());
                        context.setBornHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
                        context.setBrokerAddr(this.brokerController.getBrokerAddr());
                        context.setQueueId(requestHeader.getQueueId());
                    }

                    hook.sendMessageBefore(context);
                    if (requestHeader != null) {
                        requestHeader.setProperties(context.getMsgProps());
                    }
                } catch (AbortProcessException e) {
                    throw e;
                } catch (Throwable e) {
                    //ignore
                }
            }
        }
    }



    @Override
    public boolean rejectRequest() {
        return false;
    }
}
