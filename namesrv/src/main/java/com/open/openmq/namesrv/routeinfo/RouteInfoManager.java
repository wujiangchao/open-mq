package com.open.openmq.namesrv.routeinfo;

import com.open.openmq.common.BrokerAddrInfo;
import com.open.openmq.common.DataVersion;
import com.open.openmq.common.MixAll;
import com.open.openmq.common.TopicConfig;
import com.open.openmq.common.constant.LoggerName;
import com.open.openmq.common.constant.PermName;
import com.open.openmq.common.namesrv.NamesrvConfig;
import com.open.openmq.common.namesrv.RegisterBrokerResult;
import com.open.openmq.common.protocol.body.TopicConfigAndMappingSerializeWrapper;
import com.open.openmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.open.openmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import com.open.openmq.common.protocol.route.BrokerData;
import com.open.openmq.common.protocol.route.QueueData;
import com.open.openmq.common.protocol.route.TopicRouteData;
import com.open.openmq.common.statictopic.TopicQueueMappingInfo;
import com.open.openmq.common.topic.TopicValidator;
import com.open.openmq.common.utils.ConcurrentHashMapUtils;
import com.open.openmq.logging.InternalLogger;
import com.open.openmq.logging.InternalLoggerFactory;
import com.open.openmq.namesrv.NamesrvController;
import com.open.openmq.remoting.common.RemotingUtil;
import io.netty.channel.Channel;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Description TODO
 * @Date 2023/2/21 23:02
 * @Author jack wu
 */
public class RouteInfoManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    /**
     * Topic,以及对应的队列信息
     */
    private final Map<String/* topic */, Map<String, QueueData>> topicQueueTable;
    /**
     * 以Broker Name为单位的Broker集合
     */
    private final Map<String/* brokerName */, BrokerData> brokerAddrTable;
    /**
     * 集群以及属于该集群的Broker列表
     */
    private final Map<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;

    /**
     * 主要存储Broker的心跳发送信息，key为Broker地址，value为Broker发送心跳信息记录对象BrokerLiveInfo
     */
    private final Map<BrokerAddrInfo/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;

    /**
     * Broker对应的Filter Server列表
     */
    private final Map<BrokerAddrInfo/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

    /**
     * 主题的每个broker的队列映射信息
     */
    private final Map<String/* topic */, Map<String/*brokerName*/, TopicQueueMappingInfo>> topicQueueMappingInfoTable;

    /**
     * 定时批量移除过期Broker服务线程
     */
    private final BatchUnregistrationService unRegisterService;

    private final static long DEFAULT_BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;

    private final NamesrvController namesrvController;
    private final NamesrvConfig namesrvConfig;

    public RouteInfoManager(final NamesrvConfig namesrvConfig, NamesrvController namesrvController) {
        //topic跟queue的映射关系,存储每个topic对应的broker名称、queue数量等信息
        this.topicQueueTable = new ConcurrentHashMap<>(1024);
        //同一个BrokerName可能对应多台机器，一个Master和多个Slave。这个结构储存着一个BrokerName对应的属性信息，包括所属Cluster名称，一个Master Broker地址和多个SlaveBroker的地址等
        this.brokerAddrTable = new ConcurrentHashMap<>(128);
        //储存集群中Cluster的信息，一个cluster下的多个BrokerName
        this.clusterAddrTable = new ConcurrentHashMap<>(32);
        //存储这台Broker机器的实时状态，包括上次更新状态的时间戳，NameServer会定期检查这个时间戳，超时没有更新就认为这个Broker无效了，将其从Broker列表里清除。
        this.brokerLiveTable = new ConcurrentHashMap<>(256);
        //过滤服务器，是RocketMQ的一种服务端过滤方式，一个Broker可以有一个或多个FilterServer。这个结构的Key是Broker的地址，Value是和这个Broker关联的多个Filter Server的地址
        this.filterServerTable = new ConcurrentHashMap<>(256);
        this.topicQueueMappingInfoTable = new ConcurrentHashMap<>(1024);
        this.unRegisterService = new BatchUnregistrationService(this, namesrvConfig);
        this.namesrvConfig = namesrvConfig;
        this.namesrvController = namesrvController;
    }

    public void start() {
        this.unRegisterService.start();
    }

    public void shutdown() {
        this.unRegisterService.shutdown(true);
    }

    public boolean submitUnRegisterBrokerRequest(UnRegisterBrokerRequestHeader unRegisterRequest) {
        return this.unRegisterService.submit(unRegisterRequest);
    }

    /**
     * Broker注册
     *
     * @param clusterName        集群名称
     * @param brokerAddr         broker地址
     * @param brokerName         broker名称
     * @param brokerId           brokerid
     * @param haServerAddr       所属Master Broker的地址
     * @param zoneName           多机房场景下有用
     * @param timeoutMillis
     * @param enableActingMaster
     * @param topicConfigWrapper 主题配置
     * @param filterServerList   服务过滤列表
     * @param channel            通信channel
     * @return
     */
    public RegisterBrokerResult registerBroker(
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId,
            final String haServerAddr,
            final String zoneName,
            final Long timeoutMillis,
            final Boolean enableActingMaster,
            final TopicConfigSerializeWrapper topicConfigWrapper,
            final List<String> filterServerList,
            final Channel channel) {
        RegisterBrokerResult result = new RegisterBrokerResult();
        try {
            /**
             * 由于一个NameServer可能同时收到多个Broker的注册请求，所以在处理注册请求时使用了读写锁，在进行修改的时候添加写锁
             */
            this.lock.writeLock().lockInterruptibly();

            //init or update the cluster info    根据集群名称从clusterAddrTable获取Broker Name集合
            Set<String> brokerNames = ConcurrentHashMapUtils.computeIfAbsent((ConcurrentHashMap<String, Set<String>>) this.clusterAddrTable, clusterName, k -> new HashSet<>());
            brokerNames.add(brokerName);

            boolean registerFirst = false;

            BrokerData brokerData = this.brokerAddrTable.get(brokerName);
            //第一次注册
            if (null == brokerData) {
                registerFirst = true;
                brokerData = new BrokerData(clusterName, brokerName, new HashMap<>());
                this.brokerAddrTable.put(brokerName, brokerData);
            }

            boolean isOldVersionBroker = enableActingMaster == null;
            //如果不是老的版本，enableActingMaster = enableActingMaster，如果是老的版本enableActingMaster=false;
            brokerData.setEnableActingMaster(!isOldVersionBroker && enableActingMaster);

            brokerData.setZoneName(zoneName);

            Map<Long, String> brokerAddrsMap = brokerData.getBrokerAddrs();

            boolean isMinBrokerIdChanged = false;
            long prevMinBrokerId = 0;
            if (!brokerAddrsMap.isEmpty()) {
                prevMinBrokerId = Collections.min(brokerAddrsMap.keySet());
            }

            if (brokerId < prevMinBrokerId) {
                isMinBrokerIdChanged = true;
            }

            //Switch slave to master: first remove <1, IP:PORT> in namesrv, then add <0, IP:PORT>
            //The same IP:PORT must only have one record in brokerAddrTable
            brokerAddrsMap.entrySet().removeIf(item -> null != brokerAddr && brokerAddr.equals(item.getValue()) && brokerId != item.getKey());

            //If Local brokerId stateVersion bigger than the registering one,
            String oldBrokerAddr = brokerAddrsMap.get(brokerId);
            if (null != oldBrokerAddr && !oldBrokerAddr.equals(brokerAddr)) {
                BrokerLiveInfo oldBrokerInfo = brokerLiveTable.get(new BrokerAddrInfo(clusterName, oldBrokerAddr));

                if (null != oldBrokerInfo) {
                    long oldStateVersion = oldBrokerInfo.getDataVersion().getStateVersion();
                    long newStateVersion = topicConfigWrapper.getDataVersion().getStateVersion();
                    if (oldStateVersion > newStateVersion) {
                        log.warn("Registered Broker conflicts with the existed one, just ignore.: Cluster:{}, BrokerName:{}, BrokerId:{}, " +
                                        "Old BrokerAddr:{}, Old Version:{}, New BrokerAddr:{}, New Version:{}.",
                                clusterName, brokerName, brokerId, oldBrokerAddr, oldStateVersion, brokerAddr, newStateVersion);
                        //Remove the rejected brokerAddr from brokerLiveTable.
                        brokerLiveTable.remove(new BrokerAddrInfo(clusterName, brokerAddr));
                        return result;
                    }
                }
            }

            if (!brokerAddrsMap.containsKey(brokerId) && topicConfigWrapper.getTopicConfigTable().size() == 1) {
                log.warn("Can't register topicConfigWrapper={} because broker[{}]={} has not registered.",
                        topicConfigWrapper.getTopicConfigTable(), brokerId, brokerAddr);
                return null;
            }

            // 将新的地址信息添加到Broker地址集合中，key为Broker ID， value为Broker地址
            String oldAddr = brokerAddrsMap.put(brokerId, brokerAddr);
            // 是否首次注册
            registerFirst = registerFirst || (StringUtils.isEmpty(oldAddr));

            boolean isMaster = MixAll.MASTER_ID == brokerId;
            boolean isPrimeSlave = !isOldVersionBroker && !isMaster
                    && brokerId == Collections.min(brokerAddrsMap.keySet());

            if (null != topicConfigWrapper && (isMaster || isPrimeSlave)) {

                ConcurrentMap<String, TopicConfig> tcTable =
                        topicConfigWrapper.getTopicConfigTable();
                if (tcTable != null) {
                    for (Map.Entry<String, TopicConfig> entry : tcTable.entrySet()) {
                        if (registerFirst || this.isTopicConfigChanged(clusterName, brokerAddr,
                                topicConfigWrapper.getDataVersion(), brokerName,
                                entry.getValue().getTopicName())) {
                            final TopicConfig topicConfig = entry.getValue();
                            if (isPrimeSlave) {
                                // Wipe write perm for prime slave
                                topicConfig.setPerm(topicConfig.getPerm() & (~PermName.PERM_WRITE));
                            }
                            this.createAndUpdateQueueData(brokerName, topicConfig);
                        }
                    }
                }

                if (this.isBrokerTopicConfigChanged(clusterName, brokerAddr, topicConfigWrapper.getDataVersion()) || registerFirst) {
                    TopicConfigAndMappingSerializeWrapper mappingSerializeWrapper = TopicConfigAndMappingSerializeWrapper.from(topicConfigWrapper);
                    Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = mappingSerializeWrapper.getTopicQueueMappingInfoMap();
                    //the topicQueueMappingInfoMap should never be null, but can be empty
                    for (Map.Entry<String, TopicQueueMappingInfo> entry : topicQueueMappingInfoMap.entrySet()) {
                        if (!topicQueueMappingInfoTable.containsKey(entry.getKey())) {
                            topicQueueMappingInfoTable.put(entry.getKey(), new HashMap<>());
                        }
                        //Note asset brokerName equal entry.getValue().getBname()
                        //here use the mappingDetail.bname
                        topicQueueMappingInfoTable.get(entry.getKey()).put(entry.getValue().getBname(), entry.getValue());
                    }
                }
            }

            // 将Broker加入到brokerLiveTable中，key为Broker地址
            BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
            BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddrInfo,
                    new BrokerLiveInfo(
                            System.currentTimeMillis(),
                            timeoutMillis == null ? DEFAULT_BROKER_CHANNEL_EXPIRED_TIME : timeoutMillis,
                            topicConfigWrapper == null ? new DataVersion() : topicConfigWrapper.getDataVersion(),
                            channel,
                            haServerAddr));
            if (null == prevBrokerLiveInfo) {
                log.info("new broker registered, {} HAService: {}", brokerAddrInfo, haServerAddr);
            }

            // 处理服务过滤列表
            if (filterServerList != null) {
                if (filterServerList.isEmpty()) {
                    this.filterServerTable.remove(brokerAddrInfo);
                } else {
                    this.filterServerTable.put(brokerAddrInfo, filterServerList);
                }
            }

            // 如果发送请求的broker不是Master
            if (MixAll.MASTER_ID != brokerId) {
                // 获取Master Broker地址
                String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                if (masterAddr != null) {
                    BrokerAddrInfo masterAddrInfo = new BrokerAddrInfo(clusterName, masterAddr);
                    BrokerLiveInfo masterLiveInfo = this.brokerLiveTable.get(masterAddrInfo);
                    if (masterLiveInfo != null) {
                        // 设置HA Server地址
                        result.setHaServerAddr(masterLiveInfo.getHaServerAddr());
                        // 将Broker集群中的Master地址设置到注册结果
                        result.setMasterAddr(masterAddr);
                    }
                }
            }

            if (isMinBrokerIdChanged && namesrvConfig.isNotifyMinBrokerIdChanged()) {
//                非主线流程 先注释
//                notifyMinBrokerIdChanged(brokerAddrsMap, null,
//                        this.brokerLiveTable.get(brokerAddrInfo).getHaServerAddr());
            }
        } catch (Exception e) {
            log.error("registerBroker Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }

        return result;
    }

    public void updateBrokerInfoUpdateTimestamp(final String clusterName, final String brokerAddr) {
        BrokerAddrInfo addrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        BrokerLiveInfo prev = this.brokerLiveTable.get(addrInfo);
        if (prev != null) {
            prev.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }

    private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        queueData.setPerm(topicConfig.getPerm());
        queueData.setTopicSysFlag(topicConfig.getTopicSysFlag());

        Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topicConfig.getTopicName());
        if (null == queueDataMap) {
            queueDataMap = new HashMap<>();
            queueDataMap.put(brokerName, queueData);
            this.topicQueueTable.put(topicConfig.getTopicName(), queueDataMap);
            log.info("new topic registered, {} {}", topicConfig.getTopicName(), queueData);
        } else {
            final QueueData existedQD = queueDataMap.get(brokerName);
            if (existedQD == null) {
                queueDataMap.put(brokerName, queueData);
            } else if (!existedQD.equals(queueData)) {
                log.info("topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), existedQD,
                        queueData);
                queueDataMap.put(brokerName, queueData);
            }
        }
    }

    public boolean isTopicConfigChanged(final String clusterName, final String brokerAddr,
                                        final DataVersion dataVersion, String brokerName, String topic) {
        boolean isChange = isBrokerTopicConfigChanged(clusterName, brokerAddr, dataVersion);
        if (isChange) {
            return true;
        }
        final Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
        if (queueDataMap == null || queueDataMap.isEmpty()) {
            return true;
        }

        // The topicQueueTable already contains the broker
        return !queueDataMap.containsKey(brokerName);
    }

    public boolean isBrokerTopicConfigChanged(final String clusterName, final String brokerAddr,
                                              final DataVersion dataVersion) {
        DataVersion prev = queryBrokerTopicConfig(clusterName, brokerAddr);
        return null == prev || !prev.equals(dataVersion);
    }

    public DataVersion queryBrokerTopicConfig(final String clusterName, final String brokerAddr) {
        BrokerAddrInfo addrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        BrokerLiveInfo prev = this.brokerLiveTable.get(addrInfo);
        if (prev != null) {
            return prev.getDataVersion();
        }
        return null;
    }

    public TopicRouteData pickupTopicRouteData(final String topic) {
        TopicRouteData topicRouteData = new TopicRouteData();
        boolean foundQueueData = false;
        boolean foundBrokerData = false;
        List<BrokerData> brokerDataList = new LinkedList<>();
        topicRouteData.setBrokerDatas(brokerDataList);

        HashMap<String, List<String>> filterServerMap = new HashMap<>();
        topicRouteData.setFilterServerTable(filterServerMap);

        try {
            //加读锁
            this.lock.readLock().lockInterruptibly();
            Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
            if (queueDataMap != null) {
                topicRouteData.setQueueDatas(new ArrayList<>(queueDataMap.values()));
                foundQueueData = true;

                Set<String> brokerNameSet = new HashSet<>(queueDataMap.keySet());

                for (String brokerName : brokerNameSet) {
                    BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                    if (null == brokerData) {
                        continue;
                    }
                    BrokerData brokerDataClone = new BrokerData(brokerData.getCluster(),
                            brokerData.getBrokerName(),
                            (HashMap<Long, String>) brokerData.getBrokerAddrs().clone(),
                            brokerData.isEnableActingMaster(), brokerData.getZoneName());

                    brokerDataList.add(brokerDataClone);
                    foundBrokerData = true;
                    if (filterServerTable.isEmpty()) {
                        continue;
                    }
                    for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                        BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(brokerDataClone.getCluster(), brokerAddr);
                        List<String> filterServerList = this.filterServerTable.get(brokerAddrInfo);
                        filterServerMap.put(brokerAddr, filterServerList);
                    }
                }
            }
        } catch (Exception e) {
            log.error("pickupTopicRouteData Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);

        if (foundBrokerData && foundQueueData) {

            if (topicRouteData == null) {
                return null;
            }

            topicRouteData.setTopicQueueMappingByBroker(this.topicQueueMappingInfoTable.get(topic));

            if (!namesrvConfig.isSupportActingMaster()) {
                return topicRouteData;
            }

            if (topic.startsWith(TopicValidator.SYNC_BROKER_MEMBER_GROUP_PREFIX)) {
                return topicRouteData;
            }

            if (topicRouteData.getBrokerDatas().size() == 0 || topicRouteData.getQueueDatas().size() == 0) {
                return topicRouteData;
            }

            boolean needActingMaster = false;

            for (final BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                if (brokerData.getBrokerAddrs().size() != 0
                        && !brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                    needActingMaster = true;
                    break;
                }
            }

            if (!needActingMaster) {
                return topicRouteData;
            }

            for (final BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                final HashMap<Long, String> brokerAddrs = brokerData.getBrokerAddrs();
                if (brokerAddrs.size() == 0 || brokerAddrs.containsKey(MixAll.MASTER_ID) || !brokerData.isEnableActingMaster()) {
                    continue;
                }

                // No master
                for (final QueueData queueData : topicRouteData.getQueueDatas()) {
                    if (queueData.getBrokerName().equals(brokerData.getBrokerName())) {
                        if (!PermName.isWriteable(queueData.getPerm())) {
                            final Long minBrokerId = Collections.min(brokerAddrs.keySet());
                            final String actingMasterAddr = brokerAddrs.remove(minBrokerId);
                            brokerAddrs.put(MixAll.MASTER_ID, actingMasterAddr);
                        }
                        break;
                    }
                }
            }
            return topicRouteData;
        }
        return null;
    }

    public void scanNotActiveBroker() {
        try {
            log.info("start scanNotActiveBroker");
            for (Map.Entry<BrokerAddrInfo, BrokerLiveInfo> next : this.brokerLiveTable.entrySet()) {
                long last = next.getValue().getLastUpdateTimestamp();
                long timeoutMillis = next.getValue().getHeartbeatTimeoutMillis();
                if ((last + timeoutMillis) < System.currentTimeMillis()) {
                    RemotingUtil.closeChannel(next.getValue().getChannel());
                    log.warn("The broker channel expired, {} {}ms", next.getKey(), timeoutMillis);
                    this.onChannelDestroy(next.getKey());
                }
            }
        } catch (Exception e) {
            log.error("scanNotActiveBroker exception", e);
        }
    }


//    public void unRegisterBroker(Set<UnRegisterBrokerRequestHeader> unRegisterRequests) {
//        try {
//            Set<String> removedBroker = new HashSet<>();
//            Set<String> reducedBroker = new HashSet<>();
//            Map<String, BrokerStatusChangeInfo> needNotifyBrokerMap = new HashMap<>();
//
//            this.lock.writeLock().lockInterruptibly();
//            for (final UnRegisterBrokerRequestHeader unRegisterRequest : unRegisterRequests) {
//                final String brokerName = unRegisterRequest.getBrokerName();
//                final String clusterName = unRegisterRequest.getClusterName();
//
//                BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(clusterName, unRegisterRequest.getBrokerAddr());
//
//                BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.remove(brokerAddrInfo);
//                log.info("unregisterBroker, remove from brokerLiveTable {}, {}",
//                        brokerLiveInfo != null ? "OK" : "Failed",
//                        brokerAddrInfo
//                );
//
//                this.filterServerTable.remove(brokerAddrInfo);
//
//                boolean removeBrokerName = false;
//                boolean isMinBrokerIdChanged = false;
//                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
//                if (null != brokerData) {
//                    if (!brokerData.getBrokerAddrs().isEmpty() &&
//                            unRegisterRequest.getBrokerId().equals(Collections.min(brokerData.getBrokerAddrs().keySet()))) {
//                        isMinBrokerIdChanged = true;
//                    }
//                    String addr = brokerData.getBrokerAddrs().remove(unRegisterRequest.getBrokerId());
//                    log.info("unregisterBroker, remove addr from brokerAddrTable {}, {}",
//                            addr != null ? "OK" : "Failed",
//                            brokerAddrInfo
//                    );
//                    if (brokerData.getBrokerAddrs().isEmpty()) {
//                        this.brokerAddrTable.remove(brokerName);
//                        log.info("unregisterBroker, remove name from brokerAddrTable OK, {}",
//                                brokerName
//                        );
//
//                        removeBrokerName = true;
//                    } else if (isMinBrokerIdChanged) {
//                        needNotifyBrokerMap.put(brokerName, new BrokerStatusChangeInfo(
//                                brokerData.getBrokerAddrs(), addr, null));
//                    }
//                }
//
//                if (removeBrokerName) {
//                    Set<String> nameSet = this.clusterAddrTable.get(clusterName);
//                    if (nameSet != null) {
//                        boolean removed = nameSet.remove(brokerName);
//                        log.info("unregisterBroker, remove name from clusterAddrTable {}, {}",
//                                removed ? "OK" : "Failed",
//                                brokerName);
//
//                        if (nameSet.isEmpty()) {
//                            this.clusterAddrTable.remove(clusterName);
//                            log.info("unregisterBroker, remove cluster from clusterAddrTable {}",
//                                    clusterName
//                            );
//                        }
//                    }
//                    removedBroker.add(brokerName);
//                } else {
//                    reducedBroker.add(brokerName);
//                }
//            }
//
//            cleanTopicByUnRegisterRequests(removedBroker, reducedBroker);
//
//            if (!needNotifyBrokerMap.isEmpty() && namesrvConfig.isNotifyMinBrokerIdChanged()) {
//                notifyMinBrokerIdChanged(needNotifyBrokerMap);
//            }
//        } catch (Exception e) {
//            log.error("unregisterBroker Exception", e);
//        } finally {
//            this.lock.writeLock().unlock();
//        }
//    }


    private void cleanTopicByUnRegisterRequests(Set<String> removedBroker, Set<String> reducedBroker) {
        Iterator<Map.Entry<String, Map<String, QueueData>>> itMap = this.topicQueueTable.entrySet().iterator();
        while (itMap.hasNext()) {
            Map.Entry<String, Map<String, QueueData>> entry = itMap.next();

            String topic = entry.getKey();
            Map<String, QueueData> queueDataMap = entry.getValue();

            for (final String brokerName : removedBroker) {
                final QueueData removedQD = queueDataMap.remove(brokerName);
                if (removedQD != null) {
                    log.debug("removeTopicByBrokerName, remove one broker's topic {} {}", topic, removedQD);
                }
            }

            if (queueDataMap.isEmpty()) {
                log.debug("removeTopicByBrokerName, remove the topic all queue {}", topic);
                itMap.remove();
            }

            for (final String brokerName : reducedBroker) {
                final QueueData queueData = queueDataMap.get(brokerName);

                if (queueData != null) {
                    if (this.brokerAddrTable.get(brokerName).isEnableActingMaster()) {
                        // Master has been unregistered, wipe the write perm
                        if (isNoMasterExists(brokerName)) {
                            queueData.setPerm(queueData.getPerm() & (~PermName.PERM_WRITE));
                        }
                    }
                }
            }
        }
    }


    private boolean isNoMasterExists(String brokerName) {
        final BrokerData brokerData = this.brokerAddrTable.get(brokerName);
        if (brokerData == null) {
            return true;
        }

        if (brokerData.getBrokerAddrs().size() == 0) {
            return true;
        }

        return Collections.min(brokerData.getBrokerAddrs().keySet()) > 0;
    }

    public void onChannelDestroy(BrokerAddrInfo brokerAddrInfo) {
        UnRegisterBrokerRequestHeader unRegisterRequest = new UnRegisterBrokerRequestHeader();
        boolean needUnRegister = false;
        if (brokerAddrInfo != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    needUnRegister = setupUnRegisterRequest(unRegisterRequest, brokerAddrInfo);
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }

        if (needUnRegister) {
            boolean result = this.submitUnRegisterBrokerRequest(unRegisterRequest);
            log.info("the broker's channel destroyed, submit the unregister request at once, " +
                    "broker info: {}, submit result: {}", unRegisterRequest, result);
        }
    }

    public void onChannelDestroy(Channel channel) {
        UnRegisterBrokerRequestHeader unRegisterRequest = new UnRegisterBrokerRequestHeader();
        BrokerAddrInfo brokerAddrFound = null;
        boolean needUnRegister = false;
        if (channel != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    for (Map.Entry<BrokerAddrInfo, BrokerLiveInfo> entry : this.brokerLiveTable.entrySet()) {
                        if (entry.getValue().getChannel() == channel) {
                            brokerAddrFound = entry.getKey();
                            break;
                        }
                    }

                    if (brokerAddrFound != null) {
                        needUnRegister = setupUnRegisterRequest(unRegisterRequest, brokerAddrFound);
                    }
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }

        if (needUnRegister) {
            boolean result = this.submitUnRegisterBrokerRequest(unRegisterRequest);
            log.info("the broker's channel destroyed, submit the unregister request at once, " +
                    "broker info: {}, submit result: {}", unRegisterRequest, result);
        }
    }

    private boolean setupUnRegisterRequest(UnRegisterBrokerRequestHeader unRegisterRequest,
                                           BrokerAddrInfo brokerAddrInfo) {
        unRegisterRequest.setClusterName(brokerAddrInfo.getClusterName());
        unRegisterRequest.setBrokerAddr(brokerAddrInfo.getBrokerAddr());

        for (Map.Entry<String, BrokerData> stringBrokerDataEntry : this.brokerAddrTable.entrySet()) {
            BrokerData brokerData = stringBrokerDataEntry.getValue();
            if (!brokerAddrInfo.getClusterName().equals(brokerData.getCluster())) {
                continue;
            }

            for (Map.Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
                Long brokerId = entry.getKey();
                String brokerAddr = entry.getValue();
                if (brokerAddr.equals(brokerAddrInfo.getBrokerAddr())) {
                    unRegisterRequest.setBrokerName(brokerData.getBrokerName());
                    unRegisterRequest.setBrokerId(brokerId);
                    return true;
                }
            }
        }

        return false;
    }


    class BrokerLiveInfo {

        private long heartbeatTimeoutMillis;
        /**
         * 收到心跳发送的时间
         */
        private long lastUpdateTimestamp;
        /**
         * 版本
         */
        private DataVersion dataVersion;
        /**
         * 通信Channel
         */
        private Channel channel;
        /**
         * HA Server地址，也就是所属Master Broker的地址
         */
        private String haServerAddr;

        public BrokerLiveInfo(long lastUpdateTimestamp, long heartbeatTimeoutMillis, DataVersion dataVersion,
                              Channel channel,
                              String haServerAddr) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
            this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
            this.dataVersion = dataVersion;
            this.channel = channel;
            this.haServerAddr = haServerAddr;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        public long getHeartbeatTimeoutMillis() {
            return heartbeatTimeoutMillis;
        }

        public void setHeartbeatTimeoutMillis(long heartbeatTimeoutMillis) {
            this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
        }

        public DataVersion getDataVersion() {
            return dataVersion;
        }

        public void setDataVersion(DataVersion dataVersion) {
            this.dataVersion = dataVersion;
        }

        public Channel getChannel() {
            return channel;
        }

        public void setChannel(Channel channel) {
            this.channel = channel;
        }

        public String getHaServerAddr() {
            return haServerAddr;
        }

        public void setHaServerAddr(String haServerAddr) {
            this.haServerAddr = haServerAddr;
        }

        @Override
        public String toString() {
            return "BrokerLiveInfo [lastUpdateTimestamp=" + lastUpdateTimestamp + ", dataVersion=" + dataVersion
                    + ", channel=" + channel + ", haServerAddr=" + haServerAddr + "]";
        }
    }
}
