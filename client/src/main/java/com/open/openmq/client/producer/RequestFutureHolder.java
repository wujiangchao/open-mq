package com.open.openmq.client.producer;

import com.open.openmq.client.common.ClientErrorCode;
import com.open.openmq.client.exception.RequestTimeoutException;
import com.open.openmq.client.impl.producer.DefaultMQProducerImpl;
import com.open.openmq.client.log.ClientLogger;
import com.open.openmq.common.ThreadFactoryImpl;
import com.open.openmq.logging.InternalLogger;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Description RequestFutureHolder类是一个用于处理异步消息发送的类。它主要用于处理消息发送过程中可能出现的异常情况，例如消息发送失败、网络异常等。
 * 封装消息发送的结果：RequestFutureHolder类封装了消息发送的结果，包括发送是否成功、发送失败的原因等信息。
 * 处理消息发送的异常情况：当消息发送失败时，RequestFutureHolder类会记录失败的原因，并将异常信息返回给调用方。
 * 调用方可以根据返回的异常信息进行相应的处理。
 * 提供回调函数：RequestFutureHolder类提供了一个回调函数，用于在消息发送完成后通知调用方。
 * 调用方可以在回调函数中处理消息发送完成后的逻辑，例如向业务系统发送消息等。
 * 总之，RequestFutureHolder类在RocketMQ中主要用于处理异步消息发送的异常情况和提供回调函数，
 * 方便调用方处理消息发送的结果和逻辑。
 * @Date 2023/6/7 14:05
 * @Author jack wu
 */
public class RequestFutureHolder {
    private static InternalLogger log = ClientLogger.getLog();
    private static final RequestFutureHolder INSTANCE = new RequestFutureHolder();
    private ConcurrentHashMap<String, RequestResponseFuture> requestFutureTable = new ConcurrentHashMap<String, RequestResponseFuture>();
    private final Set<DefaultMQProducerImpl> producerSet = new HashSet<DefaultMQProducerImpl>();
    private ScheduledExecutorService scheduledExecutorService = null;

    public ConcurrentHashMap<String, RequestResponseFuture> getRequestFutureTable() {
        return requestFutureTable;
    }

    public synchronized void startScheduledTask(DefaultMQProducerImpl producer) {
        this.producerSet.add(producer);
        if (null == scheduledExecutorService) {
            this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("RequestHouseKeepingService"));

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        RequestFutureHolder.getInstance().scanExpiredRequest();
                    } catch (Throwable e) {
                        log.error("scan RequestFutureTable exception", e);
                    }
                }
            }, 1000 * 3, 1000, TimeUnit.MILLISECONDS);

        }
    }

    public static RequestFutureHolder getInstance() {
        return INSTANCE;
    }

    private void scanExpiredRequest() {
        final List<RequestResponseFuture> rfList = new LinkedList<RequestResponseFuture>();
        Iterator<Map.Entry<String, RequestResponseFuture>> it = requestFutureTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, RequestResponseFuture> next = it.next();
            RequestResponseFuture rep = next.getValue();

            if (rep.isTimeout()) {
                it.remove();
                rfList.add(rep);
                log.warn("remove timeout request, CorrelationId={}" + rep.getCorrelationId());
            }
        }

        for (RequestResponseFuture rf : rfList) {
            try {
                Throwable cause = new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, "request timeout, no reply message.");
                rf.setCause(cause);
                rf.executeRequestCallback();
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

}
