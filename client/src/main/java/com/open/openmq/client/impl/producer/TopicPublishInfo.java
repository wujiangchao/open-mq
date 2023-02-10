package com.open.openmq.client.impl.producer;

/**
 * @Description TODO
 * @Date 2023/2/11 4:10
 * @Author jack wu
 */
public class TopicPublishInfo {
    //是否有序
    private boolean orderTopic = false;
    //是否有topic路由信息
    private boolean haveTopicRouterInfo = false;
    //消息队列
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();
    //消息队列索引
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    //路由元数据
    private TopicRouteData topicRouteData;
}
