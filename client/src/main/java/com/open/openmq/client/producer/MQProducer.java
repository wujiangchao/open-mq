package com.open.openmq.client.producer;

import com.open.openmq.client.MQAdmin;
import com.open.openmq.client.exception.MQClientException;

/**
 * @Description
 * @Date 2023/1/30 23:49
 * @Author jack wu
 */
public interface MQProducer extends MQAdmin {
    void start() throws MQClientException;

    void shutdown();

    SendResult send(final Message msg) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

    SendResult send(final Message msg, final long timeout) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    void send(final Message msg, final SendCallback sendCallback) throws MQClientException,
            RemotingException, InterruptedException;

    void send(final Message msg, final SendCallback sendCallback, final long timeout)
            throws MQClientException, RemotingException, InterruptedException;

    void sendOneway(final Message msg) throws MQClientException, RemotingException,
            InterruptedException;

    SendResult send(final Message msg, final MessageQueue mq) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    SendResult send(final Message msg, final MessageQueue mq, final long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException;

    void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException;

    void sendOneway(final Message msg, final MessageQueue mq) throws MQClientException,
            RemotingException, InterruptedException;

    SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg,
                    final long timeout) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

    void send(final Message msg, final MessageQueueSelector selector, final Object arg,
              final SendCallback sendCallback) throws MQClientException, RemotingException,
            InterruptedException;

    void send(final Message msg, final MessageQueueSelector selector, final Object arg,
              final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException,
            InterruptedException;

    void sendOneway(final Message msg, final MessageQueueSelector selector, final Object arg)
            throws MQClientException, RemotingException, InterruptedException;

    TransactionSendResult sendMessageInTransaction(final Message msg,
                                                   final LocalTransactionExecuter tranExecuter, final Object arg) throws MQClientException;

    TransactionSendResult sendMessageInTransaction(final Message msg,
                                                   final Object arg) throws MQClientException;

    //for batch
    SendResult send(final Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

    SendResult send(final Collection<Message> msgs, final long timeout) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    SendResult send(final Collection<Message> msgs, final MessageQueue mq) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    SendResult send(final Collection<Message> msgs, final MessageQueue mq, final long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    void send(final Collection<Message> msgs, final SendCallback sendCallback) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

    void send(final Collection<Message> msgs, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException;

    void send(final Collection<Message> msgs, final MessageQueue mq, final SendCallback sendCallback) throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException;

    void send(final Collection<Message> msgs, final MessageQueue mq, final SendCallback sendCallback, final long timeout) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    //for rpc
    Message request(final Message msg, final long timeout) throws RequestTimeoutException, MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    void request(final Message msg, final RequestCallback requestCallback, final long timeout)
            throws MQClientException, RemotingException, InterruptedException, MQBrokerException;

    Message request(final Message msg, final MessageQueueSelector selector, final Object arg,
                    final long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

    void request(final Message msg, final MessageQueueSelector selector, final Object arg,
                 final RequestCallback requestCallback,
                 final long timeout) throws MQClientException, RemotingException,
            InterruptedException, MQBrokerException;

    Message request(final Message msg, final MessageQueue mq, final long timeout)
            throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException;

    void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;
}
