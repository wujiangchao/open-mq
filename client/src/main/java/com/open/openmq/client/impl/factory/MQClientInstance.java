package com.open.openmq.client.impl.factory;

import com.open.openmq.client.ClientConfig;
import com.open.openmq.client.exception.MQClientException;
import com.open.openmq.client.impl.MQClientAPIImpl;
import com.open.openmq.client.impl.producer.MQProducerInner;
import com.open.openmq.client.impl.producer.TopicPublishInfo;
import com.open.openmq.client.producer.DefaultMQProducer;
import com.open.openmq.common.message.MessageQueue;
import com.open.openmq.common.protocol.route.BrokerData;
import com.open.openmq.common.protocol.route.QueueData;
import com.open.openmq.common.protocol.route.TopicRouteData;
import com.open.openmq.remoting.exception.RemotingException;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @Description TODO
 * @Date 2023/2/14 14:13
 * @Author jack wu
 */
public class MQClientInstance {
    private final ClientConfig clientConfig;
    private final String clientId;
    private final long bootTimestamp = System.currentTimeMillis();
    private final MQClientAPIImpl mQClientAPIImpl;


    /**
     * The container of the producer in the current client. The key is the name of producerGroup.
     */
    private final ConcurrentMap<String, MQProducerInner> producerTable = new ConcurrentHashMap<>();

    /**
     * The container of the consumer in the current client. The key is the name of consumerGroup.
     */
    private final ConcurrentMap<String, MQConsumerInner> consumerTable = new ConcurrentHashMap<>();

    /**
     * The container of the adminExt in the current client. The key is the name of adminExtGroup.
     */
    private final ConcurrentMap<String, MQAdminExtInner> adminExtTable = new ConcurrentHashMap<>();

    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        return updateTopicRouteInfoFromNameServer(topic, false, null);
    }


    public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault,
                                                      DefaultMQProducer defaultMQProducer) {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData;
                    //isDefault???true???????????????????????????
                    if (isDefault && defaultMQProducer != null) {
                        //???nameServer??????topicRoute
                        topicRouteData = this.mQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(defaultMQProducer.getCreateTopicKey(),
                                clientConfig.getMqClientApiTimeout());
                        if (topicRouteData != null) {
                            for (QueueData data : topicRouteData.getQueueDatas()) {
                                int queueNums = Math.min(defaultMQProducer.getDefaultTopicQueueNums(), data.getReadQueueNums());
                                //????????????????????????????????????????????? ????????????????????????????????????????????????????????????????????????
                                data.setReadQueueNums(queueNums);
                                data.setWriteQueueNums(queueNums);
                            }
                        }
                    } else {
                        //????????????????????????
                        topicRouteData = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, clientConfig.getMqClientApiTimeout());
                    }
                    if (topicRouteData != null) {
                        TopicRouteData old = this.topicRouteTable.get(topic);
                        //????????????????????????????????????????????????????????????
                        boolean changed = topicRouteData.topicRouteDataChanged(old);
                        if (!changed) {
                            changed = this.isNeedUpdateTopicRouteInfo(topic);
                        } else {
                            log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                        }

                        if (changed) {

                            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                            }

                            // Update endpoint map
                            {
                                ConcurrentMap<MessageQueue, String> mqEndPoints = topicRouteData2EndpointsForStaticTopic(topic, topicRouteData);
                                if (!mqEndPoints.isEmpty()) {
                                    topicEndPointsTable.put(topic, mqEndPoints);
                                }
                            }

                            // Update Pub info
                            {
                                //??????????????????
                                TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                publishInfo.setHaveTopicRouterInfo(true);
                                for (Map.Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
                                    MQProducerInner impl = entry.getValue();
                                    if (impl != null) {
                                        //??????topic??????????????????MQProducerInner???????????????
                                        impl.updateTopicPublishInfo(topic, publishInfo);
                                    }
                                }
                            }

                            // Update sub info
                            if (!consumerTable.isEmpty()) {
                                //??????????????????
                                Set<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                                for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
                                    MQConsumerInner impl = entry.getValue();
                                    if (impl != null) {
                                        impl.updateTopicSubscribeInfo(topic, subscribeInfo);
                                    }
                                }
                            }
                            TopicRouteData cloneTopicRouteData = new TopicRouteData(topicRouteData);
                            log.info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);
                            this.topicRouteTable.put(topic, cloneTopicRouteData);
                            return true;
                        }
                    } else {
                        log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}. [{}]", topic, this.clientId);
                    }
                } catch (MQClientException e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && !topic.equals(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
                        log.warn("updateTopicRouteInfoFromNameServer Exception", e);
                    }
                } catch (RemotingException e) {
                    log.error("updateTopicRouteInfoFromNameServer Exception", e);
                    throw new IllegalStateException(e);
                } finally {
                    this.lockNamesrv.unlock();
                }
            } else {
                log.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms. [{}]", LOCK_TIMEOUT_MILLIS, this.clientId);
            }
        } catch (InterruptedException e) {
            log.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }
        return false;
    }

}
