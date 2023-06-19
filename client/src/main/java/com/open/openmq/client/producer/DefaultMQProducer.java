package com.open.openmq.client.producer;

import com.open.openmq.client.ClientConfig;
import com.open.openmq.client.exception.MQBrokerException;
import com.open.openmq.client.exception.MQClientException;
import com.open.openmq.client.exception.RequestTimeoutException;
import com.open.openmq.client.impl.producer.DefaultMQProducerImpl;
import com.open.openmq.client.impl.producer.SendCallback;
import com.open.openmq.client.log.ClientLogger;
import com.open.openmq.client.trace.TraceDispatcher;
import com.open.openmq.common.message.Message;
import com.open.openmq.common.message.MessageExt;
import com.open.openmq.common.message.MessageQueue;
import com.open.openmq.common.protocol.ResponseCode;
import com.open.openmq.common.topic.TopicValidator;
import com.open.openmq.logging.InternalLogger;
import com.open.openmq.remoting.RPCHook;
import com.open.openmq.remoting.exception.RemotingException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @Description 这个类是有意发送消息的应用程序的入口点。
 * 调整暴露了getter/setter方法的字段是可以的，但请记住，所有这些方法在大多数情况下都应该能够正常工作。
 * 这个类聚合了各种send方法，以将消息传递给代理(s)。每个方法都有优点和缺点；在实际编码之前，您最好了解它们的优点和缺点。
 * 线程安全性： 在配置和启动进程后，这个类可以被视为线程安全的，并且可以在多个线程的上下文中使用。
 * @Date 2023/1/30 23:48
 * @Author jack wu
 */
public class DefaultMQProducer extends ClientConfig implements MQProducer {

    /**
     * 对本类中呈现的几乎所有方法进行内部实现的封装。
     */
    protected final transient DefaultMQProducerImpl defaultMQProducerImpl;

    private final InternalLogger log = ClientLogger.getLog();

    private String createTopicKey = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;


    private String producerGroup;

    private TraceDispatcher traceDispatcher = null;

    private final Set<Integer> retryResponseCodes = new CopyOnWriteArraySet<Integer>(Arrays.asList(
            ResponseCode.TOPIC_NOT_EXIST,
            ResponseCode.SERVICE_NOT_AVAILABLE,
            ResponseCode.SYSTEM_ERROR,
            ResponseCode.NO_PERMISSION,
            ResponseCode.NO_BUYER_ID,
            ResponseCode.NOT_IN_CURRENT_UNIT
    ));


    /**
     * Number of queues to create per default topic.
     */
    private volatile int defaultTopicQueueNums = 4;

    /**
     * Timeout for sending messages.
     */
    private int sendMsgTimeout = 3000;

    /**
     * Compress message body threshold, namely, message body larger than 4k will be compressed on default.
     */
    private int compressMsgBodyOverHowmuch = 1024 * 4;

    /**
     * Maximum number of retry to perform internally before claiming sending failure in synchronous mode. </p>
     *
     * This may potentially cause message duplication which is up to application developers to resolve.
     */
    private int retryTimesWhenSendFailed = 2;

    /**
     * Maximum number of retry to perform internally before claiming sending failure in asynchronous mode. </p>
     *
     * This may potentially cause message duplication which is up to application developers to resolve.
     */
    private int retryTimesWhenSendAsyncFailed = 2;

    /**
     * Indicate whether to retry another broker on sending failure internally.
     */
    private boolean retryAnotherBrokerWhenNotStoreOK = false;

    /**
     * Maximum allowed message body size in bytes.
     */
    private int maxMessageSize = 1024 * 1024 * 4;


    /**
     * on BackpressureForAsyncMode, limit maximum number of on-going sending async messages
     * default is 10000
     */
    private int backPressureForAsyncSendNum = 10000;

    /**
     * on BackpressureForAsyncMode, limit maximum message size of on-going sending async messages
     * default is 100M
     */
    private int backPressureForAsyncSendSize = 100 * 1024 * 1024;

    /**
     * Constructor specifying producer group.
     *
     * @param producerGroup Producer group, see the name-sake field.
     */
    public DefaultMQProducer(final String producerGroup) {
        this(null, producerGroup, null);
    }

    /**
     * Constructor specifying namespace, producer group and RPC hook.
     *
     * @param namespace     Namespace for this MQ Producer instance.
     * @param producerGroup Producer group, see the name-sake field.
     * @param rpcHook       RPC hook to execute per each remoting command execution.
     */
    public DefaultMQProducer(final String namespace, final String producerGroup, RPCHook rpcHook) {
        this.namespace = namespace;
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
    }

    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return 0;
    }

    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return 0;
    }

    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return 0;
    }

    @Override
    public MessageExt viewMessage(String offsetMsgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return null;
    }

    @Override
    public void start() throws MQClientException {
        this.setProducerGroup(withNamespace(this.producerGroup));
        this.defaultMQProducerImpl.start();
        if (null != traceDispatcher) {
            try {
                traceDispatcher.start(this.getNamesrvAddr());
            } catch (MQClientException e) {
                log.warn("trace dispatcher start failed ", e);
            }
        }
    }

    @Override
    public void shutdown() {

    }

    /**
     * 只发送消息，queue的选择由默认的算法来实现
     *
     * @param msg
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    @Override
    public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg);
    }

    @Override
    public SendResult send(Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return null;
    }

    @Override
    public void send(Message msg, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {

    }

    @Override
    public void send(Message msg, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {

    }

    @Override
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {

    }

    /**
     * 自定义选择queue的算法进行发消息
     *
     * @param msg
     * @param mq
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return null;
    }

    @Override
    public SendResult send(Message msg, MessageQueue mq, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return null;
    }

    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {

    }

    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {

    }

    @Override
    public void sendOneway(Message msg, MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {

    }

    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return null;
    }

    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return null;
    }

    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {

    }

    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {

    }

    @Override
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException, RemotingException, InterruptedException {

    }

    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, Object arg) throws MQClientException {
        return null;
    }

    @Override
    public SendResult send(Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        //quick
        return null;
        //return this.defaultMQProducerImpl.send(batch(msgs));
    }

    @Override
    public SendResult send(Collection<Message> msgs, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return null;
    }

    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue mq) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return null;
    }

    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue mq, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return null;
    }

    @Override
    public void send(Collection<Message> msgs, SendCallback sendCallback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

    }

    @Override
    public void send(Collection<Message> msgs, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

    }

    @Override
    public void send(Collection<Message> msgs, MessageQueue mq, SendCallback sendCallback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

    }

    @Override
    public void send(Collection<Message> msgs, MessageQueue mq, SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

    }

    @Override
    public Message request(Message msg, long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return null;
    }

    @Override
    public void request(Message msg, RequestCallback requestCallback, long timeout) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

    }

    @Override
    public Message request(Message msg, MessageQueueSelector selector, Object arg, long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return null;
    }

    @Override
    public void request(Message msg, MessageQueueSelector selector, Object arg, RequestCallback requestCallback, long timeout) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

    }

    @Override
    public Message request(Message msg, MessageQueue mq, long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return null;
    }

    @Override
    public void request(Message msg, MessageQueue mq, RequestCallback requestCallback, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

    }

    public DefaultMQProducerImpl getDefaultMQProducerImpl() {
        return defaultMQProducerImpl;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public int getBackPressureForAsyncSendNum() {
        return backPressureForAsyncSendNum;
    }

    public void setBackPressureForAsyncSendNum(int backPressureForAsyncSendNum) {
        this.backPressureForAsyncSendNum = backPressureForAsyncSendNum;
    }

    public int getBackPressureForAsyncSendSize() {
        return backPressureForAsyncSendSize;
    }

    public void setBackPressureForAsyncSendSize(int backPressureForAsyncSendSize) {
        this.backPressureForAsyncSendSize = backPressureForAsyncSendSize;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public int getSendMsgTimeout() {
        return sendMsgTimeout;
    }

    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }

    public int getCompressMsgBodyOverHowmuch() {
        return compressMsgBodyOverHowmuch;
    }

    public void setCompressMsgBodyOverHowmuch(int compressMsgBodyOverHowmuch) {
        this.compressMsgBodyOverHowmuch = compressMsgBodyOverHowmuch;
    }

    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public int getRetryTimesWhenSendAsyncFailed() {
        return retryTimesWhenSendAsyncFailed;
    }

    public void setRetryTimesWhenSendAsyncFailed(int retryTimesWhenSendAsyncFailed) {
        this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
    }

    public boolean isRetryAnotherBrokerWhenNotStoreOK() {
        return retryAnotherBrokerWhenNotStoreOK;
    }

    public void setRetryAnotherBrokerWhenNotStoreOK(boolean retryAnotherBrokerWhenNotStoreOK) {
        this.retryAnotherBrokerWhenNotStoreOK = retryAnotherBrokerWhenNotStoreOK;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public Set<Integer> getRetryResponseCodes() {
        return retryResponseCodes;
    }

    public String getCreateTopicKey() {
        return createTopicKey;
    }

    public void setCreateTopicKey(String createTopicKey) {
        this.createTopicKey = createTopicKey;
    }
}
