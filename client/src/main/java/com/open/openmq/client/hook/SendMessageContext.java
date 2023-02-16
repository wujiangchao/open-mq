package com.open.openmq.client.hook;

import com.open.openmq.client.impl.CommunicationMode;
import com.open.openmq.client.impl.producer.DefaultMQProducerImpl;
import com.open.openmq.client.producer.SendResult;
import com.open.openmq.common.message.Message;
import com.open.openmq.common.message.MessageQueue;
import com.open.openmq.common.message.MessageType;

import java.util.Map;

/**
 * @Description TODO
 * @Date 2023/2/16 21:27
 * @Author jack wu
 */
public class SendMessageContext {
    private String producerGroup;
    private Message message;
    private MessageQueue mq;
    private String brokerAddr;
    private String bornHost;
    private CommunicationMode communicationMode;
    private SendResult sendResult;
    private Exception exception;
    private Object mqTraceContext;
    private Map<String, String> props;
    private DefaultMQProducerImpl producer;
    private MessageType msgType = MessageType.Normal_Msg;
    private String namespace;

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public MessageQueue getMq() {
        return mq;
    }

    public void setMq(MessageQueue mq) {
        this.mq = mq;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public String getBornHost() {
        return bornHost;
    }

    public void setBornHost(String bornHost) {
        this.bornHost = bornHost;
    }

    public CommunicationMode getCommunicationMode() {
        return communicationMode;
    }

    public void setCommunicationMode(CommunicationMode communicationMode) {
        this.communicationMode = communicationMode;
    }

    public SendResult getSendResult() {
        return sendResult;
    }

    public void setSendResult(SendResult sendResult) {
        this.sendResult = sendResult;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public Object getMqTraceContext() {
        return mqTraceContext;
    }

    public void setMqTraceContext(Object mqTraceContext) {
        this.mqTraceContext = mqTraceContext;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(Map<String, String> props) {
        this.props = props;
    }

    public DefaultMQProducerImpl getProducer() {
        return producer;
    }

    public void setProducer(DefaultMQProducerImpl producer) {
        this.producer = producer;
    }

    public MessageType getMsgType() {
        return msgType;
    }

    public void setMsgType(MessageType msgType) {
        this.msgType = msgType;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
