package com.open.openmq.common.protocol.route;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description 该类定义了topic路由的相关信息,即一个topic到哪些brokerAddr去找等等,用于网络传输
 * @Date 2023/2/18 20:42
 * @Author jack wu
 */
public class TopicRouteData {
    /**
     * topic排序的配置
     * 和"ORDER_TOPIC_CONFIG"这个NameSpace有关
     * 参照DefaultRequestProcessor#getRouteInfoByTopic
     */
    private String orderTopicConf;
    /**
     * 一个topic对应存储的位置,可参照RouteInfoManager.topicQueueTable
     */
    private List<QueueData> queueDatas;
    /**
     * 一个topic对应的brokerDatas集合(可以根据queueDatas得到，参照RouteInfoManager#pickupTopicRouteData)
     */
    private List<BrokerData> brokerDatas;
    /**
     * 每个brokerAddr对应的过滤Server地址
     */
    private HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;
    //It could be null or empty
    private Map<String/*brokerName*/, TopicQueueMappingInfo> topicQueueMappingByBroker;


    public String getOrderTopicConf() {
        return orderTopicConf;
    }

    public void setOrderTopicConf(String orderTopicConf) {
        this.orderTopicConf = orderTopicConf;
    }

    public List<QueueData> getQueueDatas() {
        return queueDatas;
    }

    public void setQueueDatas(List<QueueData> queueDatas) {
        this.queueDatas = queueDatas;
    }

    public List<BrokerData> getBrokerDatas() {
        return brokerDatas;
    }

    public void setBrokerDatas(List<BrokerData> brokerDatas) {
        this.brokerDatas = brokerDatas;
    }

    public HashMap<String, List<String>> getFilterServerTable() {
        return filterServerTable;
    }

    public void setFilterServerTable(HashMap<String, List<String>> filterServerTable) {
        this.filterServerTable = filterServerTable;
    }

    public Map<String, TopicQueueMappingInfo> getTopicQueueMappingByBroker() {
        return topicQueueMappingByBroker;
    }

    public void setTopicQueueMappingByBroker(Map<String, TopicQueueMappingInfo> topicQueueMappingByBroker) {
        this.topicQueueMappingByBroker = topicQueueMappingByBroker;
    }
}
