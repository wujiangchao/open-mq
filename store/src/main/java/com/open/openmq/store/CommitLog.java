package com.open.openmq.store;

import com.open.openmq.common.MixAll;
import com.open.openmq.common.UtilAll;
import com.open.openmq.common.constant.LoggerName;
import com.open.openmq.common.message.MessageExt;
import com.open.openmq.common.message.MessageExtBatch;
import com.open.openmq.common.message.MessageExtBrokerInner;
import com.open.openmq.logging.InternalLogger;
import com.open.openmq.logging.InternalLoggerFactory;
import com.open.openmq.store.config.BrokerRole;
import com.open.openmq.store.logfile.MappedFile;
import io.netty.buffer.ByteBuf;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * @Description 数据文件, 消息主体以及元数据的存储主体
 * @Date 2023/3/24 17:27
 * @Author jack wu
 */
public class CommitLog implements Swappable {

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);


    private final ThreadLocal<PutMessageThreadLocal> putMessageThreadLocal;

    /**
     * commitlog组成的mapfile队列，可以通过它获取正在使用的mapfile或创建新的mapfile
     */
    protected final MappedFileQueue mappedFileQueue;

    /**
     * 消息存储对象，对存储机制的再次抽象，由它统筹msg的存储逻辑。进行msg写入，主从同步，转发给indexService进行索引建立
     */
    protected final DefaultMessageStore defaultMessageStore;

//    private final FlushManager flushManager;
//
    private final AppendMessageCallback appendMessageCallback;

    protected final PutMessageLock putMessageLock;

    protected final TopicQueueLock topicQueueLock;

    private volatile long beginTimeInLock = 0;


    /**
     * 在Broker的JVM启动时，创建BrokerController对象，创建MessageStore对象，创建CommitLog
     */
    public CommitLog(final DefaultMessageStore messageStore) {
        String storePath = messageStore.getMessageStoreConfig().getStorePathCommitLog();
        // mappedFileQueue 专门管理commitlog的mapfile的对象，mapfile队列
        // 像consume queue文件也有自己的mapfile队列
        if (storePath.contains(MixAll.MULTI_PATH_SPLITTER)) {
            this.mappedFileQueue = null;
//            this.mappedFileQueue = new MultiPathMappedFileQueue(messageStore.getMessageStoreConfig(),
//                    messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
//                    messageStore.getAllocateMappedFileService(), this::getFullStorePaths);
        } else {
            this.mappedFileQueue = new MappedFileQueue(storePath,
                    messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
                    // 创建mapfile文件的线程，commitlog用来做文件提前创建和预热
                    messageStore.getAllocateMappedFileService());
        }

        this.defaultMessageStore = messageStore;

//        this.flushManager = new DefaultFlushManager();
//
//        // commitlog写入msg的实现逻辑，mapfile是抽象的文件管理对象，mapfile写入数据时只做统筹逻辑，具体的文件写入逻辑由appendMessageCallback对象回调实现
        this.appendMessageCallback = new DefaultAppendMessageCallback();
        putMessageThreadLocal = new ThreadLocal<PutMessageThreadLocal>() {
            @Override
            protected PutMessageThreadLocal initialValue() {
                return new PutMessageThreadLocal(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
            }
        };

        // 因为commitlog保存所有topic的消息，Broker接收msg是多线程并行的，存在并发写入，这里选择同步锁的实现策略，悲观锁或乐观锁
        this.putMessageLock = messageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();

//        this.flushDiskWatcher = new FlushDiskWatcher();
//
        this.topicQueueLock = new TopicQueueLock();
//
//        this.commitLogSize = messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        this.mappedFileQueue.checkSelf();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    /**
     * Broker启动，同时启动commitlog对象，让它做些事情
     * 例如启动刷盘线程，启动提交线程
     */
    public void start() {
//        this.flushManager.start();
//        log.info("start commitLog successfully. storeRoot: {}", this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
//        flushDiskWatcher.setDaemon(true);
//        flushDiskWatcher.start();
    }

    /**
     * 同样的在Broker正常关闭时做资源回收的动作，让commitlog优雅关闭线程
     */
    public void shutdown() {
//        this.flushManager.shutdown();
//        log.info("shutdown commitLog successfully. storeRoot: {}", this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
//        flushDiskWatcher.shutdown(true);
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }


    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        // Set the storage time
        //设置存储时间：isDuplicationEnable()默认false
        if (!defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            msg.setStoreTimestamp(System.currentTimeMillis());
        }

        // Set the message body CRC (consider the most appropriate setting on the client)
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // Back to Results
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        String topic = msg.getTopic();

        //发送消息的地址
        InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setBornHostV6Flag();
        }

        //存储消息的地址
        InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setStoreHostAddressV6Flag();
        }

        /**
         * 消息编码
         */
        //获取线程本地变量，其内部包含一个线程独立的encoder和keyBuilder对象
        PutMessageThreadLocal putMessageThreadLocal = this.putMessageThreadLocal.get();
        //更新最大的消息大小
        updateMaxMessageSize(putMessageThreadLocal);
        String topicQueueKey = generateKey(putMessageThreadLocal.getKeyBuilder(), msg);
        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        //从mappedFileQueue中的mappedFiles集合中获取最后一个MappedFile
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        long currOffset;
        if (mappedFile == null) {
            currOffset = 0;
        } else {
            currOffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }

        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        boolean needHandleHA = needHandleHA(msg);

//        if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
//            if (this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset) < this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas()) {
//                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
//            }
//            if (this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
//                // -1 means all ack in SyncStateSet
//                needAckNums = MixAll.ALL_ACK_IN_SYNC_STATE_SET;
//            }
//        } else if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableSlaveActingMaster()) {
//            int inSyncReplicas = Math.min(this.defaultMessageStore.getAliveReplicaNumInGroup(),
//                    this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset));
//            needAckNums = calcNeedAckNums(inSyncReplicas);
//            if (needAckNums > inSyncReplicas) {
//                // Tell the producer, don't have enough slaves to handle the send request
//                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
//            }
//        }

        topicQueueLock.lock(topicQueueKey);
        try {

            boolean needAssignOffset = true;
            if (defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()
                    && defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE) {
                needAssignOffset = false;
            }
//            if (needAssignOffset) {
//                defaultMessageStore.assignOffset(msg, getMessageNum(msg));
//            }

            //将消息内容编码，存储到encoder内部的encoderBuffer中，它是通过ByteBuffer.allocateDirect(size)得到的一个直接缓冲区
            //将消息写入之后，会调用encoderBuffer.flip()方法，将Buffer从写模式切换到读模式，可以读取到数据
            PutMessageResult encodeResult = putMessageThreadLocal.getEncoder().encode(msg);
            if (encodeResult != null) {
                return CompletableFuture.completedFuture(encodeResult);
            }
            //编码后的encoderBuffer暂时存入msg的encodeBuff中
            msg.setEncodedBuff(putMessageThreadLocal.getEncoder().getEncoderBuffer());
            //存储消息上下文
            PutMessageContext putMessageContext = new PutMessageContext(topicQueueKey);

            /**
             * 有两种锁，一种是ReentrantLock可重入锁，另一种spin则是CAS锁
             * 根据StoreConfig的useReentrantLockWhenPutMessage决定是否使用可重入锁，默认为true，使用可重入锁。
             */
            putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
            try {
                long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
                this.beginTimeInLock = beginLockTimestamp;

                // Here settings are stored timestamp, in order to ensure an orderly
                // global
                //设置存储的时间戳为加锁后的起始时间，保证有序
                if (!defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                    msg.setStoreTimestamp(beginLockTimestamp);
                }

                /**
                 * 如果最新的mappedFile为null，或者mappedFile满了，那么会新建mappedFile并返回
                 */
                if (null == mappedFile || mappedFile.isFull()) {
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
                }
                if (null == mappedFile) {
                    //log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                    //  beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null));
                }

                /**
                 * 追加存储消息
                 */
                result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
                switch (result.getStatus()) {
                    case PUT_OK:
                        onCommitLogAppend(msg, result, mappedFile);
                        break;
                    case END_OF_FILE:
                        //文件剩余空间不足，那么初始化新的文件并尝试再次存储
                        onCommitLogAppend(msg, result, mappedFile);
                        unlockMappedFile = mappedFile;
                        // Create a new file, re-write the message
                        mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                        if (null == mappedFile) {
                            // XXX: warn and notify me
                            log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                            beginTimeInLock = 0;
                            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, result));
                        }
                        result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
                        if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                            onCommitLogAppend(msg, result, mappedFile);
                        }
                        break;
                    case MESSAGE_SIZE_EXCEEDED:
                    case PROPERTIES_SIZE_EXCEEDED:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                    case UNKNOWN_ERROR:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                    default:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                }

                //加锁的持续时间
                elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
                beginTimeInLock = 0;
            } finally {
                putMessageLock.unlock();
            }
        } finally {
            topicQueueLock.unlock(topicQueueKey);
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, result);
        }

        //如果存在写满的MappedFile并且启用了文件内存预热，那么这里对MappedFile执行解锁
        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
//        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).add(result.getMsgNum());
//        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).add(result.getWroteBytes());

        //后续的处理：提交刷盘请求，提交副本请求
        return handleDiskFlushAndHA(putMessageResult, msg, needAckNums, needHandleHA);
    }

    private boolean needHandleHA(MessageExt messageExt) {

//        if (!messageExt.isWaitStoreMsgOK()) {
//            /*
//              No need to sync messages that special config to extra broker slaves.
//              @see MessageConst.PROPERTY_WAIT_STORE_MSG_OK
//             */
//            return false;
//        }

        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return false;
        }

        if (BrokerRole.SYNC_MASTER != this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            // No need to check ha in async or slave broker
            return false;
        }

        return true;
    }

    public String generateKey(StringBuilder keyBuilder, MessageExt messageExt) {
        keyBuilder.setLength(0);
        keyBuilder.append(messageExt.getTopic());
        keyBuilder.append('-');
        keyBuilder.append(messageExt.getQueueId());
        return keyBuilder.toString();
    }

    private CompletableFuture<PutMessageResult> handleDiskFlushAndHA(PutMessageResult putMessageResult,
                                                                     MessageExt messageExt, int needAckNums, boolean needHandleHA) {
        CompletableFuture<PutMessageStatus> flushResultFuture = handleDiskFlush(putMessageResult.getAppendMessageResult(), messageExt);
        CompletableFuture<PutMessageStatus> replicaResultFuture;
        if (!needHandleHA) {
            replicaResultFuture = CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        } else {
            replicaResultFuture = handleHA(putMessageResult.getAppendMessageResult(), putMessageResult, needAckNums);
        }

        return flushResultFuture.thenCombine(replicaResultFuture, (flushStatus, replicaStatus) -> {
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(flushStatus);
            }
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
            }
            return putMessageResult;
        });
    }

    private CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt) {
        return null;//this.flushManager.handleDiskFlush(result, messageExt);
    }

    private CompletableFuture<PutMessageStatus> handleHA(AppendMessageResult result, PutMessageResult putMessageResult,
                                                         int needAckNums) {
        if (needAckNums >= 0 && needAckNums <= 1) {
            return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        }

//        HAService haService = this.defaultMessageStore.getHaService();
//
//        long nextOffset = result.getWroteOffset() + result.getWroteBytes();
//
//        // Wait enough acks from different slaves
//        GroupCommitRequest request = new GroupCommitRequest(nextOffset, this.defaultMessageStore.getMessageStoreConfig().getSlaveTimeout(), needAckNums);
//        haService.putRequest(request);
//        haService.getWaitNotifyObject().wakeupAll();
//        return request.future();
        return null;
    }

    public void updateMaxMessageSize(PutMessageThreadLocal putMessageThreadLocal) {
        // dynamically adjust maxMessageSize, but not support DLedger mode temporarily
        int newMaxMessageSize = this.defaultMessageStore.getMessageStoreConfig().getMaxMessageSize();
//        if (newMaxMessageSize >= 10 &&
//                putMessageThreadLocal.getEncoder().getMaxMessageBodySize() != newMaxMessageSize) {
//            putMessageThreadLocal.getEncoder().updateEncoderBufferCapacity(newMaxMessageSize);
//        }
    }

//    private boolean needHandleHA(MessageExt messageExt) {
//
//        if (!messageExt.isWaitStoreMsgOK()) {
//            /*
//              No need to sync messages that special config to extra broker slaves.
//              @see MessageConst.PROPERTY_WAIT_STORE_MSG_OK
//             */
//            return false;
//        }
//
//        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
//            return false;
//        }
//
//        if (BrokerRole.SYNC_MASTER != this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
//            // No need to check ha in async or slave broker
//            return false;
//        }
//
//        return true;
//    }

    public static class MessageExtEncoder {
        private ByteBuf byteBuf;

        public MessageExtEncoder(int maxMessageBodySize) {
        }
        public PutMessageResult  encode(MessageExtBrokerInner msg){
            return null;

        }

        public ByteBuffer getEncoderBuffer() {
            return this.byteBuf.nioBuffer();
        }

    }

    static class PutMessageThreadLocal {
        private MessageExtEncoder encoder;
        private StringBuilder keyBuilder;

        PutMessageThreadLocal(int maxMessageBodySize) {
            encoder = new MessageExtEncoder(maxMessageBodySize);
            keyBuilder = new StringBuilder();
        }

        public MessageExtEncoder getEncoder() {
            return encoder;
        }

        public StringBuilder getKeyBuilder() {
            return keyBuilder;
        }
    }

    /**
     * 数据落地磁盘的管理，主要分为两类：实时数据刷盘(FlushRealTimeService),以及异步刷盘(GroupCommitService)
     */
    interface FlushManager {
        void start();

        void shutdown();

        void wakeUpFlush();

        void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt);

        CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt);
    }

//    class DefaultFlushManager implements FlushManager {
//        /**
//         * 刷盘线程，对commitlog的mapfile或filechannel刷盘
//         */
//        private final FlushCommitLogService flushCommitLogService;
//
//        //If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
//        //异步刷盘时，msg先写入writeBuf，再由commitLogService线程定时提交到commitlog的fileChannel中
//        private final FlushCommitLogService commitLogService;
//
//        public DefaultFlushManager() {
//            // 根据commitlog的刷盘策略，选择不同的刷盘线程实现
//            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
//                this.flushCommitLogService = new CommitLog.GroupCommitService();
//            } else {
//                this.flushCommitLogService = new CommitLog.FlushRealTimeService();
//            }
//
//            this.commitLogService = new CommitLog.CommitRealTimeService();
//        }
//
//        @Override
//        public void start() {
//            this.flushCommitLogService.start();
//
//            if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
//                this.commitLogService.start();
//            }
//        }

//        @Override
//        public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult,
//                                    MessageExt messageExt) {
//            // Synchronization flush
//            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
//                final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
//                if (messageExt.isWaitStoreMsgOK()) {
//                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
//                    service.putRequest(request);
//                    CompletableFuture<PutMessageStatus> flushOkFuture = request.future();
//                    PutMessageStatus flushStatus = null;
//                    try {
//                        flushStatus = flushOkFuture.get(CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout(),
//                                TimeUnit.MILLISECONDS);
//                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
//                        //flushOK=false;
//                    }
//                    if (flushStatus != PutMessageStatus.PUT_OK) {
//                        log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags()
//                                + " client address: " + messageExt.getBornHostString());
//                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
//                    }
//                } else {
//                    service.wakeup();
//                }
//            }
//            // Asynchronous flush
//            else {
//                if (!CommitLog.this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
//                    flushCommitLogService.wakeup();
//                } else {
//                    commitLogService.wakeup();
//                }
//            }
//        }
//
//        @Override
//        public CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt) {
//            // Synchronization flush
//            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
//                final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
//                if (messageExt.isWaitStoreMsgOK()) {
//                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
//                    flushDiskWatcher.add(request);
//                    service.putRequest(request);
//                    return request.future();
//                } else {
//                    service.wakeup();
//                    return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
//                }
//            }
//            // Asynchronous flush
//            else {
//                if (!CommitLog.this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
//                    flushCommitLogService.wakeup();
//                } else {
//                    commitLogService.wakeup();
//                }
//                return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
//            }
//        }

//        @Override
//        public void wakeUpFlush() {
//            // now wake up flush thread.
//            flushCommitLogService.wakeup();
//        }
//
//        @Override
//        public void shutdown() {
//            if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
//                this.commitLogService.shutdown();
//            }
//
//            this.flushCommitLogService.shutdown();
//        }

//    }

    protected void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile) {
        this.getMessageStore().onCommitLogAppend(msg, result, commitLogFile);
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {

        @Override
        public AppendMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, int maxBlank, MessageExtBrokerInner msg, PutMessageContext putMessageContext) {
            return null;
        }

        @Override
        public AppendMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, int maxBlank, MessageExtBatch messageExtBatch, PutMessageContext putMessageContext) {
            return null;
        }
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public MessageStore getMessageStore() {
        return defaultMessageStore;
    }
}
