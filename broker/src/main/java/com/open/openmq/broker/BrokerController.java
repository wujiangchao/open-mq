package com.open.openmq.broker;

import com.open.openmq.broker.processor.SendMessageProcessor;
import com.open.openmq.broker.slave.SlaveSynchronize;
import com.open.openmq.common.BrokerConfig;
import com.open.openmq.common.MixAll;
import com.open.openmq.common.protocol.RequestCode;
import com.open.openmq.remoting.netty.NettyClientConfig;
import com.open.openmq.remoting.netty.NettyRequestProcessor;
import com.open.openmq.remoting.netty.NettyServerConfig;
import com.open.openmq.store.MessageStore;
import com.open.openmq.store.config.MessageStoreConfig;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Description Broker核心控制
 * @Date 2023/3/22 14:28
 * @Author jack wu
 */
public class BrokerController {
    /**
     * 整个broker的配置，主要包括当前broker的名称、分组、是否master、历史内容的存储位置等
     */
    protected final BrokerConfig brokerConfig;

    /**
     * broker作为服务端启动nettyServer的配置
     */
    private final NettyServerConfig nettyServerConfig;

    /**
     * broker作为客户端的NettyClient的配置
     */
    private final NettyClientConfig nettyClientConfig;

    /**
     * 消息存储配置
     */
    protected final MessageStoreConfig messageStoreConfig;


    protected final SendMessageProcessor sendMessageProcessor;

    /**
     * salve的同步操作，如果broker是slave角色，执行同步操作，主要同步元数据信息
     */
    private final SlaveSynchronize slaveSynchronize;

    /**
     * 消息的存储功能，核心及高性能的消息存储
     */
    private MessageStore messageStore;

    /**
     * 以下是使用到的各种队列，主要是处理发送，拉取，查询等操作
     */
    protected final BlockingQueue<Runnable> sendThreadPoolQueue;
    protected final BlockingQueue<Runnable> putThreadPoolQueue;
    protected final BlockingQueue<Runnable> ackThreadPoolQueue;
    protected final BlockingQueue<Runnable> pullThreadPoolQueue;
    protected final BlockingQueue<Runnable> litePullThreadPoolQueue;
    protected final BlockingQueue<Runnable> replyThreadPoolQueue;
    protected final BlockingQueue<Runnable> queryThreadPoolQueue;
    protected final BlockingQueue<Runnable> clientManagerThreadPoolQueue;
    protected final BlockingQueue<Runnable> heartbeatThreadPoolQueue;
    protected final BlockingQueue<Runnable> consumerManagerThreadPoolQueue;
    protected final BlockingQueue<Runnable> endTransactionThreadPoolQueue;
    protected final BlockingQueue<Runnable> adminBrokerThreadPoolQueue;
    protected final BlockingQueue<Runnable> loadBalanceThreadPoolQueue;


    /**
     *  topic配置管理器，管理broker中存储的所有topic的配置
     */
    protected TopicConfigManager topicConfigManager;
    protected TopicQueueMappingManager topicQueueMappingManager;
    protected final SubscriptionGroupManager subscriptionGroupManager;
    protected final ConsumerIdsChangeListener consumerIdsChangeListener;
    /**
     * 消费者偏移量管理器，维护offset进度信息
     */
    protected final ConsumerOffsetManager consumerOffsetManager;
    /**
     * 消费者管理类，维护消费者组的注册实例信息以及topic的订阅信息，并对消费者id变化进行监听
     */
    protected final ConsumerManager consumerManager;
    protected final ConsumerFilterManager consumerFilterManager;
    protected final ConsumerOrderInfoManager consumerOrderInfoManager;

    protected ScheduledExecutorService scheduledExecutorService;
    protected final ClientHousekeepingService clientHousekeepingService;

    /**
     * Broker与外部通信 API集合
     */
    protected BrokerOuterAPI brokerOuterAPI;

    /**
     * 发送消息线程池
     */
    protected ExecutorService sendMessageExecutor;
    /**
     * 拉取消息线程池
     */
    protected ExecutorService pullMessageExecutor;
    protected ExecutorService litePullMessageExecutor;
    /**
     * Broker向NameServer发送自己信息的线程池
     */
    protected ExecutorService putMessageFutureExecutor;
    protected ExecutorService ackMessageExecutor;

    /**
     * 应答消息线程池
     */
    protected ExecutorService replyMessageExecutor;
    /**
     * 查询消息线程池
     */
    protected ExecutorService queryMessageExecutor;
    protected ExecutorService adminBrokerExecutor;
    /**
     * 客户端管理线程池
     */
    protected ExecutorService clientManageExecutor;
    /**
     * 心跳监测线程池
     */
    protected ExecutorService heartbeatExecutor;
    /**
     * consumer管理线程池
     */
    protected ExecutorService consumerManageExecutor;
    protected ExecutorService loadBalanceExecutor;
    /**
     * 事务提交或回滚的线程池
     */
    protected ExecutorService endTransactionExecutor;

    protected RemotingServer remotingServer;
    protected RemotingServer fastRemotingServer;


    public BrokerController(
            final BrokerConfig brokerConfig,
            final NettyServerConfig nettyServerConfig,
            final NettyClientConfig nettyClientConfig,
            final MessageStoreConfig messageStoreConfig
    ) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.setStoreHost(new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), getListenPort()));
        this.brokerStatsManager = messageStoreConfig.isEnableLmq() ? new LmqBrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat()) : new BrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat());
        this.consumerOffsetManager = messageStoreConfig.isEnableLmq() ? new LmqConsumerOffsetManager(this) : new ConsumerOffsetManager(this);
        this.topicConfigManager = messageStoreConfig.isEnableLmq() ? new LmqTopicConfigManager(this) : new TopicConfigManager(this);
        this.topicQueueMappingManager = new TopicQueueMappingManager(this);
        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.peekMessageProcessor = new PeekMessageProcessor(this);
        this.pullRequestHoldService = messageStoreConfig.isEnableLmq() ? new LmqPullRequestHoldService(this) : new PullRequestHoldService(this);
        this.popMessageProcessor = new PopMessageProcessor(this);
        this.notificationProcessor = new NotificationProcessor(this);
        this.pollingInfoProcessor = new PollingInfoProcessor(this);
        this.ackMessageProcessor = new AckMessageProcessor(this);
        this.changeInvisibleTimeProcessor = new ChangeInvisibleTimeProcessor(this);
        this.sendMessageProcessor = new SendMessageProcessor(this);
        this.replyMessageProcessor = new ReplyMessageProcessor(this);
        this.messageArrivingListener = new NotifyMessageArrivingListener(this.pullRequestHoldService, this.popMessageProcessor, this.notificationProcessor);
        this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        this.consumerManager = new ConsumerManager(this.consumerIdsChangeListener, this.brokerStatsManager);
        this.producerManager = new ProducerManager(this.brokerStatsManager);
        this.consumerFilterManager = new ConsumerFilterManager(this);
        this.consumerOrderInfoManager = new ConsumerOrderInfoManager(this);
        this.clientHousekeepingService = new ClientHousekeepingService(this);
        this.broker2Client = new Broker2Client(this);
        this.subscriptionGroupManager = messageStoreConfig.isEnableLmq() ? new LmqSubscriptionGroupManager(this) : new SubscriptionGroupManager(this);
        this.scheduleMessageService = new ScheduleMessageService(this);

        if (nettyClientConfig != null) {
            this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
        }

        this.filterServerManager = new FilterServerManager(this);

        this.queryAssignmentProcessor = new QueryAssignmentProcessor(this);
        this.clientManageProcessor = new ClientManageProcessor(this);
        this.slaveSynchronize = new SlaveSynchronize(this);
        this.endTransactionProcessor = new EndTransactionProcessor(this);

        this.sendThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getSendThreadPoolQueueCapacity());
        this.putThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getPutThreadPoolQueueCapacity());
        this.pullThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getPullThreadPoolQueueCapacity());
        this.litePullThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getLitePullThreadPoolQueueCapacity());

        this.ackThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getAckThreadPoolQueueCapacity());
        this.replyThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getReplyThreadPoolQueueCapacity());
        this.queryThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getQueryThreadPoolQueueCapacity());
        this.clientManagerThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getClientManagerThreadPoolQueueCapacity());
        this.consumerManagerThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getConsumerManagerThreadPoolQueueCapacity());
        this.heartbeatThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getHeartbeatThreadPoolQueueCapacity());
        this.endTransactionThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getEndTransactionPoolQueueCapacity());
        this.adminBrokerThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getAdminBrokerThreadPoolQueueCapacity());
        this.loadBalanceThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getLoadBalanceThreadPoolQueueCapacity());

        this.brokerFastFailure = new BrokerFastFailure(this);

        String brokerConfigPath;
        if (brokerConfig.getBrokerConfigPath() != null && !brokerConfig.getBrokerConfigPath().isEmpty()) {
            brokerConfigPath = brokerConfig.getBrokerConfigPath();
        } else {
            brokerConfigPath = FilenameUtils.concat(
                    FilenameUtils.getFullPathNoEndSeparator(BrokerPathConfigHelper.getBrokerConfigPath()),
                    this.brokerConfig.getCanonicalName() + ".properties");
        }
        this.configuration = new Configuration(
                LOG,
                brokerConfigPath,
                this.brokerConfig, this.nettyServerConfig, this.nettyClientConfig, this.messageStoreConfig
        );

        this.brokerStatsManager.setProduerStateGetter(new BrokerStatsManager.StateGetter() {
            @Override
            public boolean online(String instanceId, String group, String topic) {
                if (getTopicConfigManager().getTopicConfigTable().containsKey(NamespaceUtil.wrapNamespace(instanceId, topic))) {
                    return getProducerManager().groupOnline(NamespaceUtil.wrapNamespace(instanceId, group));
                } else {
                    return getProducerManager().groupOnline(group);
                }
            }
        });
        this.brokerStatsManager.setConsumerStateGetter(new BrokerStatsManager.StateGetter() {
            @Override
            public boolean online(String instanceId, String group, String topic) {
                String topicFullName = NamespaceUtil.wrapNamespace(instanceId, topic);
                if (getTopicConfigManager().getTopicConfigTable().containsKey(topicFullName)) {
                    return getConsumerManager().findSubscriptionData(NamespaceUtil.wrapNamespace(instanceId, group), topicFullName) != null;
                } else {
                    return getConsumerManager().findSubscriptionData(group, topic) != null;
                }
            }
        });

        this.brokerMemberGroup = new BrokerMemberGroup(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName());
        this.brokerMemberGroup.getBrokerAddrs().put(this.brokerConfig.getBrokerId(), this.getBrokerAddr());

        this.escapeBridge = new EscapeBridge(this);

        this.topicRouteInfoManager = new TopicRouteInfoManager(this);

        if (this.brokerConfig.isEnableSlaveActingMaster() && !this.brokerConfig.isSkipPreOnline()) {
            this.brokerPreOnlineService = new BrokerPreOnlineService(this);
        }
    }

    public void start() throws Exception{

        this.shouldStartTime = System.currentTimeMillis() + messageStoreConfig.getDisappearTimeAfterStart();

        if (messageStoreConfig.getTotalReplicas() > 1 && this.brokerConfig.isEnableSlaveActingMaster() || this.brokerConfig.isEnableControllerMode()) {
            isIsolated = true;
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.start();
        }

        startBasicService();

        if (!isIsolated && !this.messageStoreConfig.isEnableDLegerCommitLog() && !this.messageStoreConfig.isDuplicationEnable()) {
            changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MixAll.MASTER_ID);
            this.registerBrokerAll(true, false, true);
        }

        scheduledFutures.add(this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
            @Override
            public void run2() {
                try {
                    if (System.currentTimeMillis() < shouldStartTime) {
                        BrokerController.LOG.info("Register to namesrv after {}", shouldStartTime);
                        return;
                    }
                    if (isIsolated) {
                        BrokerController.LOG.info("Skip register for broker is isolated");
                        return;
                    }
                    BrokerController.this.registerBrokerAll(true, false, brokerConfig.isForceRegister());
                } catch (Throwable e) {
                    BrokerController.LOG.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, Math.max(10000, Math.min(brokerConfig.getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS));

        if (this.brokerConfig.isEnableSlaveActingMaster()) {
            scheduleSendHeartbeat();

            scheduledFutures.add(this.syncBrokerMemberGroupExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
                @Override
                public void run2() {
                    try {
                        BrokerController.this.syncBrokerMemberGroup();
                    } catch (Throwable e) {
                        BrokerController.LOG.error("sync BrokerMemberGroup error. ", e);
                    }
                }
            }, 1000, this.brokerConfig.getSyncBrokerMemberGroupPeriod(), TimeUnit.MILLISECONDS));
        }

        if (this.brokerConfig.isEnableControllerMode()) {
            scheduleSendHeartbeat();
        }

        if (brokerConfig.isSkipPreOnline()) {
            startServiceWithoutCondition();
        }
    }

    public void registerProcessor() {
        /*
         * SendMessageProcessor
         */
        sendMessageProcessor.registerSendMessageHook(sendMessageHookList);
        sendMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);

        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendMessageProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendMessageProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendMessageProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendMessageProcessor, this.sendMessageExecutor);
        /**
         * PullMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor, this.pullMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.LITE_PULL_MESSAGE, this.pullMessageProcessor, this.litePullMessageExecutor);
        this.pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);
        /**
         * PeekMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.PEEK_MESSAGE, this.peekMessageProcessor, this.pullMessageExecutor);
        /**
         * PopMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.POP_MESSAGE, this.popMessageProcessor, this.pullMessageExecutor);

        /**
         * AckMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.ACK_MESSAGE, this.ackMessageProcessor, this.ackMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.ACK_MESSAGE, this.ackMessageProcessor, this.ackMessageExecutor);
        /**
         * ChangeInvisibleTimeProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, this.changeInvisibleTimeProcessor, this.ackMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, this.changeInvisibleTimeProcessor, this.ackMessageExecutor);
        /**
         * notificationProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.NOTIFICATION, this.notificationProcessor, this.pullMessageExecutor);

        /**
         * pollingInfoProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.POLLING_INFO, this.pollingInfoProcessor, this.pullMessageExecutor);

        /**
         * ReplyMessageProcessor
         */

        replyMessageProcessor.registerSendMessageHook(sendMessageHookList);

        this.remotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE, replyMessageProcessor, replyMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE_V2, replyMessageProcessor, replyMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE, replyMessageProcessor, replyMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE_V2, replyMessageProcessor, replyMessageExecutor);

        /**
         * QueryMessageProcessor
         */
        NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.queryMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.queryMessageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.queryMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.queryMessageExecutor);

        /**
         * ClientManageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientManageProcessor, this.heartbeatExecutor);
        this.remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientManageProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientManageProcessor, this.clientManageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.HEART_BEAT, clientManageProcessor, this.heartbeatExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientManageProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientManageProcessor, this.clientManageExecutor);

        /**
         * ConsumerManageProcessor
         */
        ConsumerManageProcessor consumerManageProcessor = new ConsumerManageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);

        /**
         * QueryAssignmentProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.QUERY_ASSIGNMENT, queryAssignmentProcessor, loadBalanceExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_ASSIGNMENT, queryAssignmentProcessor, loadBalanceExecutor);
        this.remotingServer.registerProcessor(RequestCode.SET_MESSAGE_REQUEST_MODE, queryAssignmentProcessor, loadBalanceExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SET_MESSAGE_REQUEST_MODE, queryAssignmentProcessor, loadBalanceExecutor);

        /**
         * EndTransactionProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.END_TRANSACTION, endTransactionProcessor, this.endTransactionExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.END_TRANSACTION, endTransactionProcessor, this.endTransactionExecutor);

        /*
         * Default
         */
        AdminBrokerProcessor adminProcessor = new AdminBrokerProcessor(this);
        this.remotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
        this.fastRemotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
    }

    /**
     * 加载历史的内容，历史内容都是存储到本地的文件中，主要是做消息接受，分发，过滤的管理内容加载
     * 加载消息存储内容，核心重要原生的消息读取
     * 构造netty的服务，当前作为broker的服务端
     * 各种执行消息操作的线程池配置，内部是基于各个功能做线程池隔离
     * 注册事件处理机制，特别说，这里注册时处理拉取消息以外的事件处理
     * 执行各种任务调度，主要是报告当前服务情况，保存基于消息的管理内容，和namesrv的数据交互
     * 基于高可用的master，salve的配置及输出
     * 执行事务，鉴权，hooks的操作
     *
     * @return
     * @throws CloneNotSupportedException
     */
    public boolean initialize() throws CloneNotSupportedException {
        // 加载主题配置
        boolean result = this.topicConfigManager.load();
        result = result && this.topicQueueMappingManager.load();
        // 加载消息位点配置
        result = result && this.consumerOffsetManager.load();
        // 加载订阅关系配置
        result = result && this.subscriptionGroupManager.load();
        // 加载消息过滤配置
        result = result && this.consumerFilterManager.load();
        result = result && this.consumerOrderInfoManager.load();

        if (result) {
            try {
                //默认消息存储实现类
                DefaultMessageStore defaultMessageStore = new DefaultMessageStore(this.messageStoreConfig, this.brokerStatsManager, this.messageArrivingListener, this.brokerConfig);
                defaultMessageStore.setTopicConfigTable(topicConfigManager.getTopicConfigTable());

                if (messageStoreConfig.isEnableDLegerCommitLog()) {
                    DLedgerRoleChangeHandler roleChangeHandler = new DLedgerRoleChangeHandler(this, defaultMessageStore);
                    ((DLedgerCommitLog) defaultMessageStore.getCommitLog()).getdLedgerServer().getdLedgerLeaderElector().addRoleChangeHandler(roleChangeHandler);
                }
                // 初始化监控统计对象brokerStats
                this.brokerStats = new BrokerStats(defaultMessageStore);
                //load plugin
                MessageStorePluginContext context = new MessageStorePluginContext(this, messageStoreConfig, brokerStatsManager, messageArrivingListener);
                this.messageStore = MessageStoreFactory.build(context, defaultMessageStore);
                this.messageStore.getDispatcherList().addFirst(new CommitLogDispatcherCalcBitMap(this.brokerConfig, this.consumerFilterManager));
                if (this.brokerConfig.isEnableControllerMode()) {
                    this.replicasManager = new ReplicasManager(this);
                }
                if (messageStoreConfig.isTimerWheelEnable()) {
                    this.timerCheckpoint = new TimerCheckpoint(BrokerPathConfigHelper.getTimerCheckPath(messageStoreConfig.getStorePathRootDir()));
                    TimerMetrics timerMetrics = new TimerMetrics(BrokerPathConfigHelper.getTimerMetricsPath(messageStoreConfig.getStorePathRootDir()));
                    this.timerMessageStore = new TimerMessageStore(messageStore, messageStoreConfig, timerCheckpoint, timerMetrics, brokerStatsManager);
                    this.timerMessageStore.registerEscapeBridgeHook(msg -> escapeBridge.putMessage(msg));
                    this.messageStore.setTimerMessageStore(this.timerMessageStore);
                }
            } catch (IOException e) {
                result = false;
                LOG.error("BrokerController#initialize: unexpected error occurs", e);
            }
        }
        if (messageStore != null) {
            registerMessageStoreHook();
        }

        //加载历史数据
        result = result && this.messageStore.load();

        if (messageStoreConfig.isTimerWheelEnable()) {
            result = result && this.timerMessageStore.load();
        }

        //scheduleMessageService load after messageStore load success
        result = result && this.scheduleMessageService.load();

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                result = result && brokerAttachedPlugin.load();
            }
        }

        if (result) {

            initializeRemotingServer();

            initializeResources();

            registerProcessor();

            initializeScheduledTasks();
            //初始化事物管理机制
            initialTransaction();
            //初始化命令行管理执行，执行操作的管控
            initialAcl();
            //初始化rpc的hook机制
            initialRpcHooks();

            if (TlsSystemConfig.tlsMode != TlsMode.DISABLED) {
                // Register a listener to reload SslContext
                try {
                    fileWatchService = new FileWatchService(
                            new String[]{
                                    TlsSystemConfig.tlsServerCertPath,
                                    TlsSystemConfig.tlsServerKeyPath,
                                    TlsSystemConfig.tlsServerTrustCertPath
                            },
                            new FileWatchService.Listener() {
                                boolean certChanged, keyChanged = false;

                                @Override
                                public void onChanged(String path) {
                                    if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                                        LOG.info("The trust certificate changed, reload the ssl context");
                                        reloadServerSslContext();
                                    }
                                    if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                                        certChanged = true;
                                    }
                                    if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                                        keyChanged = true;
                                    }
                                    if (certChanged && keyChanged) {
                                        LOG.info("The certificate and private key changed, reload the ssl context");
                                        certChanged = keyChanged = false;
                                        reloadServerSslContext();
                                    }
                                }

                                private void reloadServerSslContext() {
                                    ((NettyRemotingServer) remotingServer).loadSslContext();
                                    ((NettyRemotingServer) fastRemotingServer).loadSslContext();
                                }
                            });
                } catch (Exception e) {
                    result = false;
                    LOG.warn("FileWatchService created error, can't load the certificate dynamically");
                }
            }
        }

        return result;
    }

    protected void initializeRemotingServer() throws CloneNotSupportedException {
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);
        NettyServerConfig fastConfig = (NettyServerConfig) this.nettyServerConfig.clone();

        int listeningPort = nettyServerConfig.getListenPort() - 2;
        if (listeningPort < 0) {
            listeningPort = 0;
        }
        fastConfig.setListenPort(listeningPort);
        //快速服务端实现 —— broker的两级服务，一个是快速提供服务操作，只是没有pull的事件处理
        this.fastRemotingServer = new NettyRemotingServer(fastConfig, this.clientHousekeepingService);
    }

    /**
     * Initialize resources including remoting server and thread executors.
     */
    protected void initializeResources() {
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryImpl("BrokerControllerScheduledThread", true, getBrokerIdentity()));

        this.sendMessageExecutor = new BrokerFixedThreadPoolExecutor(
                this.brokerConfig.getSendMessageThreadPoolNums(),
                this.brokerConfig.getSendMessageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.sendThreadPoolQueue,
                new ThreadFactoryImpl("SendMessageThread_", getBrokerIdentity()));

        this.pullMessageExecutor = new BrokerFixedThreadPoolExecutor(
                this.brokerConfig.getPullMessageThreadPoolNums(),
                this.brokerConfig.getPullMessageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.pullThreadPoolQueue,
                new ThreadFactoryImpl("PullMessageThread_", getBrokerIdentity()));

        this.litePullMessageExecutor = new BrokerFixedThreadPoolExecutor(
                this.brokerConfig.getLitePullMessageThreadPoolNums(),
                this.brokerConfig.getLitePullMessageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.litePullThreadPoolQueue,
                new ThreadFactoryImpl("LitePullMessageThread_", getBrokerIdentity()));

        this.putMessageFutureExecutor = new BrokerFixedThreadPoolExecutor(
                this.brokerConfig.getPutMessageFutureThreadPoolNums(),
                this.brokerConfig.getPutMessageFutureThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.putThreadPoolQueue,
                new ThreadFactoryImpl("SendMessageThread_", getBrokerIdentity()));

        this.ackMessageExecutor = new BrokerFixedThreadPoolExecutor(
                this.brokerConfig.getAckMessageThreadPoolNums(),
                this.brokerConfig.getAckMessageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.ackThreadPoolQueue,
                new ThreadFactoryImpl("AckMessageThread_", getBrokerIdentity()));

        this.queryMessageExecutor = new BrokerFixedThreadPoolExecutor(
                this.brokerConfig.getQueryMessageThreadPoolNums(),
                this.brokerConfig.getQueryMessageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.queryThreadPoolQueue,
                new ThreadFactoryImpl("QueryMessageThread_", getBrokerIdentity()));

        this.adminBrokerExecutor = new BrokerFixedThreadPoolExecutor(
                this.brokerConfig.getAdminBrokerThreadPoolNums(),
                this.brokerConfig.getAdminBrokerThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.adminBrokerThreadPoolQueue,
                new ThreadFactoryImpl("AdminBrokerThread_", getBrokerIdentity()));

        this.clientManageExecutor = new BrokerFixedThreadPoolExecutor(
                this.brokerConfig.getClientManageThreadPoolNums(),
                this.brokerConfig.getClientManageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.clientManagerThreadPoolQueue,
                new ThreadFactoryImpl("ClientManageThread_", getBrokerIdentity()));

        this.heartbeatExecutor = new BrokerFixedThreadPoolExecutor(
                this.brokerConfig.getHeartbeatThreadPoolNums(),
                this.brokerConfig.getHeartbeatThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.heartbeatThreadPoolQueue,
                new ThreadFactoryImpl("HeartbeatThread_", true, getBrokerIdentity()));

        this.consumerManageExecutor = new BrokerFixedThreadPoolExecutor(
                this.brokerConfig.getConsumerManageThreadPoolNums(),
                this.brokerConfig.getConsumerManageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.consumerManagerThreadPoolQueue,
                new ThreadFactoryImpl("ConsumerManageThread_", true, getBrokerIdentity()));

        this.replyMessageExecutor = new BrokerFixedThreadPoolExecutor(
                this.brokerConfig.getProcessReplyMessageThreadPoolNums(),
                this.brokerConfig.getProcessReplyMessageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.replyThreadPoolQueue,
                new ThreadFactoryImpl("ProcessReplyMessageThread_", getBrokerIdentity()));

        this.endTransactionExecutor = new BrokerFixedThreadPoolExecutor(
                this.brokerConfig.getEndTransactionThreadPoolNums(),
                this.brokerConfig.getEndTransactionThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.endTransactionThreadPoolQueue,
                new ThreadFactoryImpl("EndTransactionThread_", getBrokerIdentity()));

        this.loadBalanceExecutor = new BrokerFixedThreadPoolExecutor(
                this.brokerConfig.getLoadBalanceProcessorThreadPoolNums(),
                this.brokerConfig.getLoadBalanceProcessorThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.loadBalanceThreadPoolQueue,
                new ThreadFactoryImpl("LoadBalanceProcessorThread_", getBrokerIdentity()));

        this.syncBrokerMemberGroupExecutorService = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryImpl("BrokerControllerSyncBrokerScheduledThread", getBrokerIdentity()));
        this.brokerHeartbeatExecutorService = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryImpl("rokerControllerHeartbeatScheduledThread", getBrokerIdentity()));

        this.topicQueueMappingCleanService = new TopicQueueMappingCleanService(this);
    }

    protected void initializeBrokerScheduledTasks() {
        final long initialDelay = UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis();
        final long period = TimeUnit.DAYS.toMillis(1);
        //通过定时器定时记录broker的状态信息
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.getBrokerStats().record();
                } catch (Throwable e) {
                    LOG.error("BrokerController: failed to record broker stats", e);
                }
            }
        }, initialDelay, period, TimeUnit.MILLISECONDS);

        //通过定时器定时持久化consumer的偏移量内容
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.consumerOffsetManager.persist();
                } catch (Throwable e) {
                    LOG.error(
                            "BrokerController: failed to persist config file of consumerOffset", e);
                }
            }
        }, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        //通过定时器定时持久化consumer的过滤器内容
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.consumerFilterManager.persist();
                    BrokerController.this.consumerOrderInfoManager.persist();
                } catch (Throwable e) {
                    LOG.error(
                            "BrokerController: failed to persist config file of consumerFilter or consumerOrderInfo",
                            e);
                }
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);

        //通过定时器定时执行broker的保护验证机制
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.protectBroker();
                } catch (Throwable e) {
                    LOG.error("BrokerController: failed to protectBroker", e);
                }
            }
        }, 3, 3, TimeUnit.MINUTES);

        //通过定时器定时打印各个配置的内容量
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.printWaterMark();
                } catch (Throwable e) {
                    LOG.error("BrokerController: failed to print broker watermark", e);
                }
            }
        }, 10, 1, TimeUnit.SECONDS);

        //通过定时器定时打印日志内容的大小
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    LOG.info("Dispatch task fall behind commit log {}bytes",
                            BrokerController.this.getMessageStore().dispatchBehindBytes());
                } catch (Throwable e) {
                    LOG.error("Failed to print dispatchBehindBytes", e);
                }
            }
        }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);

        if (!messageStoreConfig.isEnableDLegerCommitLog() && !messageStoreConfig.isDuplicationEnable() && !brokerConfig.isEnableControllerMode()) {
            if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
                if (this.messageStoreConfig.getHaMasterAddress() != null && this.messageStoreConfig.getHaMasterAddress().length() >= HA_ADDRESS_MIN_LENGTH) {
                    this.messageStore.updateHaMasterAddress(this.messageStoreConfig.getHaMasterAddress());
                    this.updateMasterHAServerAddrPeriodically = false;
                } else {
                    this.updateMasterHAServerAddrPeriodically = true;
                }

                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            if (System.currentTimeMillis() - lastSyncTimeMs > 60 * 1000) {
                                BrokerController.this.getSlaveSynchronize().syncAll();
                                lastSyncTimeMs = System.currentTimeMillis();
                            }
                            //timer checkpoint, latency-sensitive, so sync it more frequently
                            BrokerController.this.getSlaveSynchronize().syncTimerCheckPoint();
                        } catch (Throwable e) {
                            LOG.error("Failed to sync all config for slave.", e);
                        }
                    }
                }, 1000 * 10, 3 * 1000, TimeUnit.MILLISECONDS);

            } else {
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.printMasterAndSlaveDiff();
                        } catch (Throwable e) {
                            LOG.error("Failed to print diff of master and slave.", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
            }
        }

        if (this.brokerConfig.isEnableControllerMode()) {
            this.updateMasterHAServerAddrPeriodically = true;
        }
    }

    protected void initializeScheduledTasks() {

        initializeBrokerScheduledTasks();

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.brokerOuterAPI.refreshMetadata();
                } catch (Exception e) {
                    LOG.error("ScheduledTask refresh metadata exception", e);
                }
            }
        }, 10, 5, TimeUnit.SECONDS);

        //更新name server的配置
        if (this.brokerConfig.getNamesrvAddr() != null) {
            this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
            LOG.info("Set user specified name server address: {}", this.brokerConfig.getNamesrvAddr());
            // also auto update namesrv if specify
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.brokerOuterAPI.updateNameServerAddressList(BrokerController.this.brokerConfig.getNamesrvAddr());
                    } catch (Throwable e) {
                        LOG.error("Failed to update nameServer address list", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        } else if (this.brokerConfig.isFetchNamesrvAddrByAddressServer()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        BrokerController.this.brokerOuterAPI.fetchNameServerAddr();
                    } catch (Throwable e) {
                        LOG.error("Failed to fetch nameServer address", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }
    }


    public MessageStore getMessageStore() {
        return messageStore;
    }
}
