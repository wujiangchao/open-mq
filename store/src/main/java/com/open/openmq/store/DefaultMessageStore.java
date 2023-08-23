package com.open.openmq.store;

import com.open.openmq.common.BrokerConfig;
import com.open.openmq.common.BrokerIdentity;
import com.open.openmq.common.SystemClock;
import com.open.openmq.common.ThreadFactoryImpl;
import com.open.openmq.common.TopicConfig;
import com.open.openmq.common.UtilAll;
import com.open.openmq.common.constant.LoggerName;
import com.open.openmq.common.message.MessageConst;
import com.open.openmq.common.message.MessageExtBatch;
import com.open.openmq.common.message.MessageExtBrokerInner;
import com.open.openmq.common.sysflag.MessageSysFlag;
import com.open.openmq.logging.InternalLogger;
import com.open.openmq.logging.InternalLoggerFactory;
import com.open.openmq.store.config.MessageStoreConfig;
import com.open.openmq.store.config.StorePathConfigHelper;
import com.open.openmq.store.ha.HAService;
import com.open.openmq.store.hook.PutMessageHook;
import com.open.openmq.store.logfile.MappedFile;
import com.open.openmq.store.queue.ConsumeQueueInterface;
import com.open.openmq.store.queue.ConsumeQueueStore;
import com.open.openmq.store.stast.BrokerStatsManager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;

/**
 * @Description TODO
 * @Date 2023/3/24 11:19
 * @Author jack wu
 */
public class DefaultMessageStore implements MessageStore {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);


    // Max pull msg size
    private final static int MAX_PULL_MSG_SIZE = 128 * 1024 * 1024;

    private final MessageStoreConfig messageStoreConfig;

    protected List<PutMessageHook> putMessageHookList = new ArrayList<>();

    // CommitLog（Commitlog文件存储实现类）
    private final CommitLog commitLog;

    private final ConsumeQueueStore consumeQueueStore;


    private final AllocateMappedFileService allocateMappedFileService;

    private final SystemClock systemClock = new SystemClock();

    private final StoreStatsService storeStatsService;

    private final BrokerConfig brokerConfig;

    private HAService haService;


    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
                               final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws IOException {
        this.messageStoreConfig = messageStoreConfig;
        this.allocateMappedFileService = new AllocateMappedFileService(this);
        this.commitLog = new CommitLog(this);
        this.consumeQueueStore = new ConsumeQueueStore(this, this.messageStoreConfig);
        this.storeStatsService = new StoreStatsService(getBrokerIdentity());
        this.brokerConfig = brokerConfig;
    }

    public BrokerIdentity getBrokerIdentity() {
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            return new BrokerIdentity(
                    brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                    Integer.parseInt(messageStoreConfig.getdLegerSelfId().substring(1)), brokerConfig.isInBrokerContainer());
        } else {
            return new BrokerIdentity(
                    brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                    brokerConfig.getBrokerId(), brokerConfig.isInBrokerContainer());
        }
    }



    public void setTopicConfigTable(ConcurrentMap<String, TopicConfig> topicConfigTable) {
       this.consumeQueueStore.setTopicConfigTable(topicConfigTable);
    }

    @Override
    public boolean load() {
        boolean result = true;

        try {
            boolean lastExitOK = !this.isTempFileExist();
            LOGGER.info("last shutdown {}, root dir: {}", lastExitOK ? "normally" : "abnormally", messageStoreConfig.getStorePathRootDir());

            // load Commit Log
            result = result && this.commitLog.load();

            // load Consume Queue
            result = result && this.consumeQueueStore.load();

            if (result) {
//                this.storeCheckpoint =
//                        new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
//                this.masterFlushedOffset = this.storeCheckpoint.getMasterFlushedOffset();
//                this.indexService.load(lastExitOK);
//
//                this.recover(lastExitOK);

                LOGGER.info("load over, and the max phy offset = {}", this.getMaxPhyOffset());
            }

            long maxOffset = this.getMaxPhyOffset();
            this.setBrokerInitMaxOffset(maxOffset);
            LOGGER.info("load over, and the max phy offset = {}", maxOffset);
        } catch (Exception e) {
            LOGGER.error("load exception", e);
            result = false;
        }

        if (!result) {
            this.allocateMappedFileService.shutdown();
        }

        return result;
    }




    private boolean isTempFileExist() {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        return file.exists();
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void destroy() {

    }

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        return waitForPutResult(asyncPutMessage(msg));
    }

    /**
     * 处理、存储消息
     *asyncPutMessage函数对比putMessage函数，主要区别就在于最后的消息刷盘逻辑和主从同步逻辑是并行还是串行
     * @param msg MessageInstance to store
     * @return
     */
    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {

        /**
         * 在放置消息之前执行钩子函数。例如，消息验证或特殊消息转换，并返回处理结果
         */
        for (PutMessageHook putMessageHook : putMessageHookList) {
            PutMessageResult handleResult = putMessageHook.executeBeforePutMessage(msg);
            if (handleResult != null) {
                return CompletableFuture.completedFuture(handleResult);
            }
        }

        //检查消息是否具有属性，是不是内部批处理
//        if (msg.getProperties().containsKey(MessageConst.PROPERTY_INNER_NUM)
//                && !MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
//            LOGGER.warn("[BUG]The message had property {} but is not an inner batch", MessageConst.PROPERTY_INNER_NUM);
//            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
//        }

        //消息是内部批处理，但是cq类型不是批处处理cq
//        if (MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
//            Optional<TopicConfig> topicConfig = this.getTopicConfig(msg.getTopic());
//            if (!QueueTypeUtils.isBatchCq(topicConfig)) {
//                LOGGER.error("[BUG]The message is an inner batch but cq type is not batch cq");
//                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
//            }
//        }
//
//        //当前时间戳
//        long beginTime = this.getSystemClock().now();
        CompletableFuture<PutMessageResult> putResultFuture = this.commitLog.asyncPutMessage(msg);

        putResultFuture.thenAccept(result -> {
            //存储消息消耗的时间
//            long elapsedTime = this.getSystemClock().now() - beginTime;
//            if (elapsedTime > 500) {
//                LOGGER.warn("DefaultMessageStore#putMessage: CommitLog#putMessage cost {}ms, topic={}, bodyLength={}",
//                        elapsedTime, msg.getTopic(), msg.getBody().length);
//            }
            //更新统计保存消息花费的时间和最大花费时间
//            this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);
//
//            if (null == result || !result.isOk()) {
//                //如果存储失败，则增加保存消息失败的次数
//                this.storeStatsService.getPutMessageFailedTimes().add(1);
//            }
        });

        return putResultFuture;
    }

    @Override
    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        return null;
    }

    @Override
    public long getStateMachineVersion() {
        return 0;
    }

    @Override
    public long now() {
        return systemClock.now();
    }

    @Override
    public long getMaxPhyOffset() {
        return this.commitLog.getMaxOffset();
    }

    @Override
    public long getMinPhyOffset() {
        return 0;
    }

    @Override
    public void setBrokerInitMaxOffset(long brokerInitMaxOffset) {

    }

    @Override
    public void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile) {

    }

    @Override
    public void unlockMappedFile(MappedFile unlockMappedFile) {
//        this.scheduledExecutorService.schedule(new Runnable() {
//            @Override
//            public void run() {
//                mappedFile.munlock();
//            }
//        }, 6, TimeUnit.SECONDS);
    }

    private PutMessageResult waitForPutResult(CompletableFuture<PutMessageResult> putMessageResultFuture) {
        try {
            int putMessageTimeout =
                    Math.max(this.messageStoreConfig.getSyncFlushTimeout(),
                            this.messageStoreConfig.getSlaveTimeout()) + 5000;
            return putMessageResultFuture.get(putMessageTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | InterruptedException e) {
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        } catch (TimeoutException e) {
            LOGGER.error("usually it will never timeout, putMessageTimeout is much bigger than slaveTimeout and "
                    + "flushTimeout so the result can be got anyway, but in some situations timeout will happen like full gc "
                    + "process hangs or other unexpected situations.");
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        }
    }


    @Override
    public GetMessageResult getMessage(String group, String topic, int queueId, long offset, int maxMsgNums, MessageFilter messageFilter) {
        return getMessage(group, topic, queueId, offset, maxMsgNums, MAX_PULL_MSG_SIZE, messageFilter);
    }

    @Override
    public GetMessageResult getMessage(String group, String topic, int queueId, long offset, int maxMsgNums, int maxTotalMsgSize, MessageFilter messageFilter) {
//        if (this.shutdown) {
//            LOGGER.warn("message store has shutdown, so getMessage is forbidden");
//            return null;
//        }
//
//        if (!this.runningFlags.isReadable()) {
//            LOGGER.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
//            return null;
//        }
//
//        long beginTime = this.getSystemClock().now();
//
//        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
//
//        //下次拉取的consumeQueue的起始逻辑偏移量
//        long nextBeginOffset = offset;
//        long minOffset = 0;
//        long maxOffset = 0;
//
//        GetMessageResult getResult = new GetMessageResult();
//
//        //获取commitLog的最大物理偏移量
//        final long maxOffsetPy = this.commitLog.getMaxOffset();
//
//        //根据topic和队列id确定需要写入的ConsumeQueue
//        ConsumeQueueInterface consumeQueue = findConsumeQueue(topic, queueId);
//        if (consumeQueue != null) {
//            minOffset = consumeQueue.getMinOffsetInQueue();
//            maxOffset = consumeQueue.getMaxOffsetInQueue();
//
//            if (maxOffset == 0) {
//                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
//                nextBeginOffset = nextOffsetCorrection(offset, 0);
//            } else if (offset < minOffset) {
//                status = GetMessageStatus.OFFSET_TOO_SMALL;
//                nextBeginOffset = nextOffsetCorrection(offset, minOffset);
//            } else if (offset == maxOffset) {
//                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
//                nextBeginOffset = nextOffsetCorrection(offset, offset);
//            } else if (offset > maxOffset) {
//                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
//                nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
//            } else {
//                final int maxFilterMessageCount = Math.max(16000, maxMsgNums * ConsumeQueue.CQ_STORE_UNIT_SIZE);
//                final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();
//
//                long maxPullSize = Math.max(maxTotalMsgSize, 100);
//                if (maxPullSize > MAX_PULL_MSG_SIZE) {
//                    LOGGER.warn("The max pull size is too large maxPullSize={} topic={} queueId={}", maxPullSize, topic, queueId);
//                    maxPullSize = MAX_PULL_MSG_SIZE;
//                }
//                status = GetMessageStatus.NO_MATCHED_MESSAGE;
//                long maxPhyOffsetPulling = 0;
//                int cqFileNum = 0;
//
//                while (getResult.getBufferTotalSize() <= 0
//                        && nextBeginOffset < maxOffset
//                        && cqFileNum++ < this.messageStoreConfig.getTravelCqFileNumWhenGetMessage()) {
//                    ReferredIterator<CqUnit> bufferConsumeQueue = consumeQueue.iterateFrom(nextBeginOffset);
//
//                    if (bufferConsumeQueue == null) {
//                        status = GetMessageStatus.OFFSET_FOUND_NULL;
//                        nextBeginOffset = nextOffsetCorrection(nextBeginOffset, this.consumeQueueStore.rollNextFile(consumeQueue, nextBeginOffset));
//                        LOGGER.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
//                                + maxOffset + ", but access logic queue failed. Correct nextBeginOffset to " + nextBeginOffset);
//                        break;
//                    }
//
//                    try {
//                        long nextPhyFileStartOffset = Long.MIN_VALUE;
//                        while (bufferConsumeQueue.hasNext()
//                                && nextBeginOffset < maxOffset) {
//                            CqUnit cqUnit = bufferConsumeQueue.next();
//                            long offsetPy = cqUnit.getPos();
//                            int sizePy = cqUnit.getSize();
//
//                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
//
//                            if (cqUnit.getQueueOffset() - offset > maxFilterMessageCount) {
//                                break;
//                            }
//
//                            if (this.isTheBatchFull(sizePy, cqUnit.getBatchNum(), maxMsgNums, maxPullSize, getResult.getBufferTotalSize(), getResult.getMessageCount(), isInDisk)) {
//                                break;
//                            }
//
//                            if (getResult.getBufferTotalSize() >= maxPullSize) {
//                                break;
//                            }
//
//                            maxPhyOffsetPulling = offsetPy;
//
//                            //Be careful, here should before the isTheBatchFull
//                            nextBeginOffset = cqUnit.getQueueOffset() + cqUnit.getBatchNum();
//
//                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
//                                if (offsetPy < nextPhyFileStartOffset) {
//                                    continue;
//                                }
//                            }
//
//                            if (messageFilter != null
//                                    && !messageFilter.isMatchedByConsumeQueue(cqUnit.getValidTagsCodeAsLong(), cqUnit.getCqExtUnit())) {
//                                if (getResult.getBufferTotalSize() == 0) {
//                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
//                                }
//
//                                continue;
//                            }
//
//                            SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
//                            if (null == selectResult) {
//                                if (getResult.getBufferTotalSize() == 0) {
//                                    status = GetMessageStatus.MESSAGE_WAS_REMOVING;
//                                }
//
//                                nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
//                                continue;
//                            }
//
//                            if (messageFilter != null
//                                    && !messageFilter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
//                                if (getResult.getBufferTotalSize() == 0) {
//                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
//                                }
//                                // release...
//                                selectResult.release();
//                                continue;
//                            }
//
//                            this.storeStatsService.getGetMessageTransferredMsgCount().add(cqUnit.getBatchNum());
//                            getResult.addMessage(selectResult, cqUnit.getQueueOffset(), cqUnit.getBatchNum());
//                            status = GetMessageStatus.FOUND;
//                            nextPhyFileStartOffset = Long.MIN_VALUE;
//                        }
//                    } finally {
//                        bufferConsumeQueue.release();
//                    }
//                }
//
//                if (diskFallRecorded) {
//                    long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
//                    brokerStatsManager.recordDiskFallBehindSize(group, topic, queueId, fallBehind);
//                }
//
//                long diff = maxOffsetPy - maxPhyOffsetPulling;
//                long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE
//                        * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
//                getResult.setSuggestPullingFromSlave(diff > memory);
//            }
//        } else {
//            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
//            nextBeginOffset = nextOffsetCorrection(offset, 0);
//        }
//
//        if (GetMessageStatus.FOUND == status) {
//            this.storeStatsService.getGetMessageTimesTotalFound().add(1);
//        } else {
//            this.storeStatsService.getGetMessageTimesTotalMiss().add(1);
//        }
//        long elapsedTime = this.getSystemClock().now() - beginTime;
//        this.storeStatsService.setGetMessageEntireTimeMax(elapsedTime);
//
//        // lazy init no data found.
//        if (getResult == null) {
//            getResult = new GetMessageResult(0);
//        }
//
//        getResult.setStatus(status);
//        getResult.setNextBeginOffset(nextBeginOffset);
//        getResult.setMaxOffset(maxOffset);
//        getResult.setMinOffset(minOffset);
//        return getResult;
        return null;
    }

//    @Override
//    public ConsumeQueueInterface findConsumeQueue(String topic, int queueId) {
//        return this.consumeQueueStore.findOrCreateConsumeQueue(topic, queueId);
//    }

    public String getStorePathPhysic() {
        String storePathPhysic = null;
        if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog()) {
           // storePathPhysic = ((DLedgerCommitLog) DefaultMessageStore.this.getCommitLog()).getdLedgerServer().getdLedgerConfig().getDataStorePath();
        } else {
            storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
        }
        return storePathPhysic;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public HAService getHaService() {
        return haService;
    }

    public void setHaService(HAService haService) {
        this.haService = haService;
    }

    public SystemClock getSystemClock() {
        return systemClock;
    }
}
