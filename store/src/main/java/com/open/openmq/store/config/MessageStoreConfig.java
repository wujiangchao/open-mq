package com.open.openmq.store.config;

import java.io.File;

/**
 * @Description TODO
 * @Date 2023/3/23 16:43
 * @Author jack wu
 */
public class MessageStoreConfig {
    public static final String MULTI_PATH_SPLITTER = System.getProperty("openmq.broker.multiPathSplitter", ",");

    //The root directory in which the log data is kept
    @ImportantField
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";

    //The directory in which the commitlog is kept
    @ImportantField
    private String storePathCommitLog = null;

    @ImportantField
    private String storePathDLedgerCommitLog = null;

    //The directory in which the epochFile is kept
    @ImportantField
    private String storePathEpochFile = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "epochFileCheckpoint";

    private String readOnlyCommitLogStorePaths = null;

    // CommitLog file size,default is 1G
    private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;

    // TimerLog file size, default is 100M
    private int mappedFileSizeTimerLog = 100 * 1024 * 1024;

    private int timerPrecisionMs = 1000;

    private int timerRollWindowSlot = 3600 * 24 * 2;
    private int timerFlushIntervalMs = 1000;
    private int timerGetMessageThreadNum = 3;
    private int timerPutMessageThreadNum = 3;

    private boolean timerEnableDisruptor = false;

    private boolean timerEnableCheckMetrics = true;
    private boolean timerInterceptDelayLevel = false;
    private int timerMaxDelaySec = 3600 * 24 * 3;
    private boolean timerWheelEnable = true;

    /**
     * 1. Register to broker after (startTime + disappearTimeAfterStart)
     * 2. Internal msg exchange will start after (startTime + disappearTimeAfterStart)
     *  A. PopReviveService
     *  B. TimerDequeueGetService
     */
    @ImportantField
    private int disappearTimeAfterStart = -1;

    private boolean timerStopEnqueue = false;

    private String timerCheckMetricsWhen = "05";

    private boolean timerSkipUnknownError = false;
    private boolean timerWarmEnable = false;
    private boolean timerStopDequeue = false;
    private int timerCongestNumEachSlot = Integer.MAX_VALUE;

    private int timerMetricSmallThreshold = 1000000;
    private int timerProgressLogIntervalMs = 10 * 1000;

    // ConsumeQueue file size,default is 30W
    private int mappedFileSizeConsumeQueue = 300000 * ConsumeQueue.CQ_STORE_UNIT_SIZE;
    // enable consume queue ext
    private boolean enableConsumeQueueExt = false;
    // ConsumeQueue extend file size, 48M
    private int mappedFileSizeConsumeQueueExt = 48 * 1024 * 1024;
    private int mapperFileSizeBatchConsumeQueue = 300000 * BatchConsumeQueue.CQ_STORE_UNIT_SIZE;
    // Bit count of filter bit map.
    // this will be set by pipe of calculate filter bit map.
    private int bitMapLengthConsumeQueueExt = 64;

    // CommitLog flush interval
    // flush data to disk
    @ImportantField
    private int flushIntervalCommitLog = 500;

    // Only used if TransientStorePool enabled
    // flush data to FileChannel
    @ImportantField
    private int commitIntervalCommitLog = 200;

    private int maxRecoveryCommitlogFiles = 30;

    private int diskSpaceWarningLevelRatio = 90;

    private int diskSpaceCleanForciblyRatio = 85;

    /**
     * introduced since 4.0.x. Determine whether to use mutex reentrantLock when putting message.<br/>
     */
    private boolean useReentrantLockWhenPutMessage = true;

    // Whether schedule flush
    @ImportantField
    private boolean flushCommitLogTimed = true;
    // ConsumeQueue flush interval
    private int flushIntervalConsumeQueue = 1000;
    // Resource reclaim interval
    private int cleanResourceInterval = 10000;
    // CommitLog removal interval
    private int deleteCommitLogFilesInterval = 100;
    // ConsumeQueue removal interval
    private int deleteConsumeQueueFilesInterval = 100;
    private int destroyMapedFileIntervalForcibly = 1000 * 120;
    private int redeleteHangedFileInterval = 1000 * 120;
    // When to delete,default is at 4 am
    @ImportantField
    private String deleteWhen = "04";
    private int diskMaxUsedSpaceRatio = 75;
    // The number of hours to keep a log file before deleting it (in hours)
    @ImportantField
    private int fileReservedTime = 72;
    @ImportantField
    private int deleteFileBatchMax = 10;
    // Flow control for ConsumeQueue
    private int putMsgIndexHightWater = 600000;
    // The maximum size of message body,default is 4M,4M only for body length,not include others.
    private int maxMessageSize = 1024 * 1024 * 4;
    // Whether check the CRC32 of the records consumed.
    // This ensures no on-the-wire or on-disk corruption to the messages occurred.
    // This check adds some overhead,so it may be disabled in cases seeking extreme performance.
    private boolean checkCRCOnRecover = true;
    // How many pages are to be flushed when flush CommitLog
    private int flushCommitLogLeastPages = 4;
    // How many pages are to be committed when commit data to file
    private int commitCommitLogLeastPages = 4;
    // Flush page size when the disk in warming state
    private int flushLeastPagesWhenWarmMapedFile = 1024 / 4 * 16;
    // How many pages are to be flushed when flush ConsumeQueue
    private int flushConsumeQueueLeastPages = 2;
    private int flushCommitLogThoroughInterval = 1000 * 10;
    private int commitCommitLogThoroughInterval = 200;
    private int flushConsumeQueueThoroughInterval = 1000 * 60;
    @ImportantField
    private int maxTransferBytesOnMessageInMemory = 1024 * 256;
    @ImportantField
    private int maxTransferCountOnMessageInMemory = 32;
    @ImportantField
    private int maxTransferBytesOnMessageInDisk = 1024 * 64;
    @ImportantField
    private int maxTransferCountOnMessageInDisk = 8;
    @ImportantField
    private int accessMessageInMemoryMaxRatio = 40;
    @ImportantField
    private boolean messageIndexEnable = true;
    private int maxHashSlotNum = 5000000;
    private int maxIndexNum = 5000000 * 4;
    private int maxMsgsNumBatch = 64;
    @ImportantField
    private boolean messageIndexSafe = false;
    private int haListenPort = 10912;
    private int haSendHeartbeatInterval = 1000 * 5;
    private int haHousekeepingInterval = 1000 * 20;
    /**
     * Maximum size of data to transfer to slave.
     * NOTE: cannot be larger than HAClient.READ_MAX_BUFFER_SIZE
     */
    private int haTransferBatchSize = 1024 * 32;
    @ImportantField
    private String haMasterAddress = null;
    private int haMaxGapNotInSync = 1024 * 1024 * 256;
    @ImportantField
    private volatile BrokerRole brokerRole = BrokerRole.ASYNC_MASTER;
    @ImportantField
    private FlushDiskType flushDiskType = FlushDiskType.ASYNC_FLUSH;
    // Used by GroupTransferService to sync messages from master to slave
    private int syncFlushTimeout = 1000 * 5;
    // Used by PutMessage to wait messages be flushed to disk and synchronized in current broker member group.
    private int putMessageTimeout = 1000 * 8;
    private int slaveTimeout = 3000;
    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
    private long flushDelayOffsetInterval = 1000 * 10;
    @ImportantField
    private boolean cleanFileForciblyEnable = true;
    private boolean warmMapedFileEnable = false;
    private boolean offsetCheckInSlave = false;
    private boolean debugLockEnable = false;
    private boolean duplicationEnable = false;
    private boolean diskFallRecorded = true;
    private long osPageCacheBusyTimeOutMills = 1000;
    private int defaultQueryMaxNum = 32;

    @ImportantField
    private boolean transientStorePoolEnable = false;
    private int transientStorePoolSize = 5;
    private boolean fastFailIfNoBufferInStorePool = false;

    // DLedger message store config
    private boolean enableDLegerCommitLog = false;
    private String dLegerGroup;
    private String dLegerPeers;
    private String dLegerSelfId;
    private String preferredLeaderId;
    private boolean isEnableBatchPush = false;

    private boolean enableScheduleMessageStats = true;

    private boolean enableLmq = false;
    private boolean enableMultiDispatch = false;
    private int maxLmqConsumeQueueNum = 20000;

    private boolean enableScheduleAsyncDeliver = false;
    private int scheduleAsyncDeliverMaxPendingLimit = 2000;
    private int scheduleAsyncDeliverMaxResendNum2Blocked = 3;

    private int maxBatchDeleteFilesNum = 50;
    //Polish dispatch
    private int dispatchCqThreads = 10;
    private int dispatchCqCacheNum = 1024 * 4;
    private boolean enableAsyncReput = true;
    //For recheck the reput
    private boolean recheckReputOffsetFromCq = false;

    // Maximum length of topic
    private int maxTopicLength = 1000;
    private int travelCqFileNumWhenGetMessage = 1;
    // Sleep interval between to corrections
    private int correctLogicMinOffsetSleepInterval = 1;
    // Force correct min offset interval
    private int correctLogicMinOffsetForceInterval = 5 * 60 * 1000;
    // swap
    private boolean mappedFileSwapEnable = true;
    private long commitLogForceSwapMapInterval = 12L * 60 * 60 * 1000;
    private long commitLogSwapMapInterval = 1L * 60 * 60 * 1000;
    private int commitLogSwapMapReserveFileNum = 100;
    private long logicQueueForceSwapMapInterval = 12L * 60 * 60 * 1000;
    private long logicQueueSwapMapInterval = 1L * 60 * 60 * 1000;
    private long cleanSwapedMapInterval = 5L * 60 * 1000;
    private int logicQueueSwapMapReserveFileNum = 20;

    private boolean searchBcqByCacheEnable = true;

    @ImportantField
    private boolean dispatchFromSenderThread = false;

    @ImportantField
    private boolean wakeCommitWhenPutMessage = true;
    @ImportantField
    private boolean wakeFlushWhenPutMessage = false;

    @ImportantField
    private boolean enableCleanExpiredOffset = false;

    private int maxAsyncPutMessageRequests = 5000;

    private int pullBatchMaxMessageCount = 160;

    @ImportantField
    private int totalReplicas = 1;

    /**
     * Each message must be written successfully to at least in-sync replicas.
     * The master broker is considered one of the in-sync replicas, and it's included in the count of total.
     * If a master broker is ASYNC_MASTER, inSyncReplicas will be ignored.
     * If enableControllerMode is true and ackAckInSyncStateSet is true, inSyncReplicas will be ignored.
     */
    @ImportantField
    private int inSyncReplicas = 1;

    /**
     * Will be worked in auto multiple replicas mode, to provide minimum in-sync replicas.
     * It is still valid in controller mode.
     */
    @ImportantField
    private int minInSyncReplicas = 1;

    /**
     * Each message must be written successfully to all replicas in InSyncStateSet.
     */
    @ImportantField
    private boolean allAckInSyncStateSet = false;

    /**
     * Dynamically adjust in-sync replicas to provide higher availability, the real time in-sync replicas
     * will smaller than inSyncReplicas config.
     */
    @ImportantField
    private boolean enableAutoInSyncReplicas = false;

    /**
     * Enable or not ha flow control
     */
    @ImportantField
    private boolean haFlowControlEnable = false;

    /**
     * The max speed for one slave when transfer data in ha
     */
    private long maxHaTransferByteInSecond = 100 * 1024 * 1024;

    /**
     * The max gap time that slave doesn't catch up to master.
     */
    private long haMaxTimeSlaveNotCatchup = 1000 * 15;

    /**
     * Sync flush offset from master when broker startup, used in upgrading from old version broker.
     */
    private boolean syncMasterFlushOffsetWhenStartup = false;

    /**
     * Max checksum range.
     */
    private long maxChecksumRange = 1024 * 1024 * 1024;

    private int replicasPerDiskPartition = 1;

    private double logicalDiskSpaceCleanForciblyThreshold = 0.8;

    private long maxSlaveResendLength = 256 * 1024 * 1024;

    /**
     * Whether sync from lastFile when a new broker replicas(no data) join the master.
     */
    private boolean syncFromLastFile = false;

    private boolean asyncLearner = false;

    public String getStorePathRootDir() {
        return storePathRootDir;
    }

    public void setStorePathRootDir(String storePathRootDir) {
        this.storePathRootDir = storePathRootDir;
    }

    public String getStorePathCommitLog() {
        return storePathCommitLog;
    }

    public void setStorePathCommitLog(String storePathCommitLog) {
        this.storePathCommitLog = storePathCommitLog;
    }

    public String getStorePathDLedgerCommitLog() {
        return storePathDLedgerCommitLog;
    }

    public void setStorePathDLedgerCommitLog(String storePathDLedgerCommitLog) {
        this.storePathDLedgerCommitLog = storePathDLedgerCommitLog;
    }

    public String getStorePathEpochFile() {
        return storePathEpochFile;
    }

    public void setStorePathEpochFile(String storePathEpochFile) {
        this.storePathEpochFile = storePathEpochFile;
    }

    public String getReadOnlyCommitLogStorePaths() {
        return readOnlyCommitLogStorePaths;
    }

    public void setReadOnlyCommitLogStorePaths(String readOnlyCommitLogStorePaths) {
        this.readOnlyCommitLogStorePaths = readOnlyCommitLogStorePaths;
    }

    public int getMappedFileSizeCommitLog() {
        return mappedFileSizeCommitLog;
    }

    public void setMappedFileSizeCommitLog(int mappedFileSizeCommitLog) {
        this.mappedFileSizeCommitLog = mappedFileSizeCommitLog;
    }

    public int getMappedFileSizeTimerLog() {
        return mappedFileSizeTimerLog;
    }

    public void setMappedFileSizeTimerLog(int mappedFileSizeTimerLog) {
        this.mappedFileSizeTimerLog = mappedFileSizeTimerLog;
    }

    public int getTimerPrecisionMs() {
        return timerPrecisionMs;
    }

    public void setTimerPrecisionMs(int timerPrecisionMs) {
        this.timerPrecisionMs = timerPrecisionMs;
    }

    public int getTimerRollWindowSlot() {
        return timerRollWindowSlot;
    }

    public void setTimerRollWindowSlot(int timerRollWindowSlot) {
        this.timerRollWindowSlot = timerRollWindowSlot;
    }

    public int getTimerFlushIntervalMs() {
        return timerFlushIntervalMs;
    }

    public void setTimerFlushIntervalMs(int timerFlushIntervalMs) {
        this.timerFlushIntervalMs = timerFlushIntervalMs;
    }

    public int getTimerGetMessageThreadNum() {
        return timerGetMessageThreadNum;
    }

    public void setTimerGetMessageThreadNum(int timerGetMessageThreadNum) {
        this.timerGetMessageThreadNum = timerGetMessageThreadNum;
    }

    public int getTimerPutMessageThreadNum() {
        return timerPutMessageThreadNum;
    }

    public void setTimerPutMessageThreadNum(int timerPutMessageThreadNum) {
        this.timerPutMessageThreadNum = timerPutMessageThreadNum;
    }

    public boolean isTimerEnableDisruptor() {
        return timerEnableDisruptor;
    }

    public void setTimerEnableDisruptor(boolean timerEnableDisruptor) {
        this.timerEnableDisruptor = timerEnableDisruptor;
    }

    public boolean isTimerEnableCheckMetrics() {
        return timerEnableCheckMetrics;
    }

    public void setTimerEnableCheckMetrics(boolean timerEnableCheckMetrics) {
        this.timerEnableCheckMetrics = timerEnableCheckMetrics;
    }

    public boolean isTimerInterceptDelayLevel() {
        return timerInterceptDelayLevel;
    }

    public void setTimerInterceptDelayLevel(boolean timerInterceptDelayLevel) {
        this.timerInterceptDelayLevel = timerInterceptDelayLevel;
    }

    public int getTimerMaxDelaySec() {
        return timerMaxDelaySec;
    }

    public void setTimerMaxDelaySec(int timerMaxDelaySec) {
        this.timerMaxDelaySec = timerMaxDelaySec;
    }

    public boolean isTimerWheelEnable() {
        return timerWheelEnable;
    }

    public void setTimerWheelEnable(boolean timerWheelEnable) {
        this.timerWheelEnable = timerWheelEnable;
    }

    public int getDisappearTimeAfterStart() {
        return disappearTimeAfterStart;
    }

    public void setDisappearTimeAfterStart(int disappearTimeAfterStart) {
        this.disappearTimeAfterStart = disappearTimeAfterStart;
    }

    public boolean isTimerStopEnqueue() {
        return timerStopEnqueue;
    }

    public void setTimerStopEnqueue(boolean timerStopEnqueue) {
        this.timerStopEnqueue = timerStopEnqueue;
    }

    public String getTimerCheckMetricsWhen() {
        return timerCheckMetricsWhen;
    }

    public void setTimerCheckMetricsWhen(String timerCheckMetricsWhen) {
        this.timerCheckMetricsWhen = timerCheckMetricsWhen;
    }

    public boolean isTimerSkipUnknownError() {
        return timerSkipUnknownError;
    }

    public void setTimerSkipUnknownError(boolean timerSkipUnknownError) {
        this.timerSkipUnknownError = timerSkipUnknownError;
    }

    public boolean isTimerWarmEnable() {
        return timerWarmEnable;
    }

    public void setTimerWarmEnable(boolean timerWarmEnable) {
        this.timerWarmEnable = timerWarmEnable;
    }

    public boolean isTimerStopDequeue() {
        return timerStopDequeue;
    }

    public void setTimerStopDequeue(boolean timerStopDequeue) {
        this.timerStopDequeue = timerStopDequeue;
    }

    public int getTimerCongestNumEachSlot() {
        return timerCongestNumEachSlot;
    }

    public void setTimerCongestNumEachSlot(int timerCongestNumEachSlot) {
        this.timerCongestNumEachSlot = timerCongestNumEachSlot;
    }

    public int getTimerMetricSmallThreshold() {
        return timerMetricSmallThreshold;
    }

    public void setTimerMetricSmallThreshold(int timerMetricSmallThreshold) {
        this.timerMetricSmallThreshold = timerMetricSmallThreshold;
    }

    public int getTimerProgressLogIntervalMs() {
        return timerProgressLogIntervalMs;
    }

    public void setTimerProgressLogIntervalMs(int timerProgressLogIntervalMs) {
        this.timerProgressLogIntervalMs = timerProgressLogIntervalMs;
    }

    public int getMappedFileSizeConsumeQueue() {
        return mappedFileSizeConsumeQueue;
    }

    public void setMappedFileSizeConsumeQueue(int mappedFileSizeConsumeQueue) {
        this.mappedFileSizeConsumeQueue = mappedFileSizeConsumeQueue;
    }

    public boolean isEnableConsumeQueueExt() {
        return enableConsumeQueueExt;
    }

    public void setEnableConsumeQueueExt(boolean enableConsumeQueueExt) {
        this.enableConsumeQueueExt = enableConsumeQueueExt;
    }

    public int getMappedFileSizeConsumeQueueExt() {
        return mappedFileSizeConsumeQueueExt;
    }

    public void setMappedFileSizeConsumeQueueExt(int mappedFileSizeConsumeQueueExt) {
        this.mappedFileSizeConsumeQueueExt = mappedFileSizeConsumeQueueExt;
    }

    public int getMapperFileSizeBatchConsumeQueue() {
        return mapperFileSizeBatchConsumeQueue;
    }

    public void setMapperFileSizeBatchConsumeQueue(int mapperFileSizeBatchConsumeQueue) {
        this.mapperFileSizeBatchConsumeQueue = mapperFileSizeBatchConsumeQueue;
    }

    public int getBitMapLengthConsumeQueueExt() {
        return bitMapLengthConsumeQueueExt;
    }

    public void setBitMapLengthConsumeQueueExt(int bitMapLengthConsumeQueueExt) {
        this.bitMapLengthConsumeQueueExt = bitMapLengthConsumeQueueExt;
    }

    public int getFlushIntervalCommitLog() {
        return flushIntervalCommitLog;
    }

    public void setFlushIntervalCommitLog(int flushIntervalCommitLog) {
        this.flushIntervalCommitLog = flushIntervalCommitLog;
    }

    public int getCommitIntervalCommitLog() {
        return commitIntervalCommitLog;
    }

    public void setCommitIntervalCommitLog(int commitIntervalCommitLog) {
        this.commitIntervalCommitLog = commitIntervalCommitLog;
    }

    public int getMaxRecoveryCommitlogFiles() {
        return maxRecoveryCommitlogFiles;
    }

    public void setMaxRecoveryCommitlogFiles(int maxRecoveryCommitlogFiles) {
        this.maxRecoveryCommitlogFiles = maxRecoveryCommitlogFiles;
    }

    public int getDiskSpaceWarningLevelRatio() {
        return diskSpaceWarningLevelRatio;
    }

    public void setDiskSpaceWarningLevelRatio(int diskSpaceWarningLevelRatio) {
        this.diskSpaceWarningLevelRatio = diskSpaceWarningLevelRatio;
    }

    public int getDiskSpaceCleanForciblyRatio() {
        return diskSpaceCleanForciblyRatio;
    }

    public void setDiskSpaceCleanForciblyRatio(int diskSpaceCleanForciblyRatio) {
        this.diskSpaceCleanForciblyRatio = diskSpaceCleanForciblyRatio;
    }

    public boolean isUseReentrantLockWhenPutMessage() {
        return useReentrantLockWhenPutMessage;
    }

    public void setUseReentrantLockWhenPutMessage(boolean useReentrantLockWhenPutMessage) {
        this.useReentrantLockWhenPutMessage = useReentrantLockWhenPutMessage;
    }

    public boolean isFlushCommitLogTimed() {
        return flushCommitLogTimed;
    }

    public void setFlushCommitLogTimed(boolean flushCommitLogTimed) {
        this.flushCommitLogTimed = flushCommitLogTimed;
    }

    public int getFlushIntervalConsumeQueue() {
        return flushIntervalConsumeQueue;
    }

    public void setFlushIntervalConsumeQueue(int flushIntervalConsumeQueue) {
        this.flushIntervalConsumeQueue = flushIntervalConsumeQueue;
    }

    public int getCleanResourceInterval() {
        return cleanResourceInterval;
    }

    public void setCleanResourceInterval(int cleanResourceInterval) {
        this.cleanResourceInterval = cleanResourceInterval;
    }

    public int getDeleteCommitLogFilesInterval() {
        return deleteCommitLogFilesInterval;
    }

    public void setDeleteCommitLogFilesInterval(int deleteCommitLogFilesInterval) {
        this.deleteCommitLogFilesInterval = deleteCommitLogFilesInterval;
    }

    public int getDeleteConsumeQueueFilesInterval() {
        return deleteConsumeQueueFilesInterval;
    }

    public void setDeleteConsumeQueueFilesInterval(int deleteConsumeQueueFilesInterval) {
        this.deleteConsumeQueueFilesInterval = deleteConsumeQueueFilesInterval;
    }

    public int getDestroyMapedFileIntervalForcibly() {
        return destroyMapedFileIntervalForcibly;
    }

    public void setDestroyMapedFileIntervalForcibly(int destroyMapedFileIntervalForcibly) {
        this.destroyMapedFileIntervalForcibly = destroyMapedFileIntervalForcibly;
    }

    public int getRedeleteHangedFileInterval() {
        return redeleteHangedFileInterval;
    }

    public void setRedeleteHangedFileInterval(int redeleteHangedFileInterval) {
        this.redeleteHangedFileInterval = redeleteHangedFileInterval;
    }

    public String getDeleteWhen() {
        return deleteWhen;
    }

    public void setDeleteWhen(String deleteWhen) {
        this.deleteWhen = deleteWhen;
    }

    public int getDiskMaxUsedSpaceRatio() {
        return diskMaxUsedSpaceRatio;
    }

    public void setDiskMaxUsedSpaceRatio(int diskMaxUsedSpaceRatio) {
        this.diskMaxUsedSpaceRatio = diskMaxUsedSpaceRatio;
    }

    public int getFileReservedTime() {
        return fileReservedTime;
    }

    public void setFileReservedTime(int fileReservedTime) {
        this.fileReservedTime = fileReservedTime;
    }

    public int getDeleteFileBatchMax() {
        return deleteFileBatchMax;
    }

    public void setDeleteFileBatchMax(int deleteFileBatchMax) {
        this.deleteFileBatchMax = deleteFileBatchMax;
    }

    public int getPutMsgIndexHightWater() {
        return putMsgIndexHightWater;
    }

    public void setPutMsgIndexHightWater(int putMsgIndexHightWater) {
        this.putMsgIndexHightWater = putMsgIndexHightWater;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public boolean isCheckCRCOnRecover() {
        return checkCRCOnRecover;
    }

    public void setCheckCRCOnRecover(boolean checkCRCOnRecover) {
        this.checkCRCOnRecover = checkCRCOnRecover;
    }

    public int getFlushCommitLogLeastPages() {
        return flushCommitLogLeastPages;
    }

    public void setFlushCommitLogLeastPages(int flushCommitLogLeastPages) {
        this.flushCommitLogLeastPages = flushCommitLogLeastPages;
    }

    public int getCommitCommitLogLeastPages() {
        return commitCommitLogLeastPages;
    }

    public void setCommitCommitLogLeastPages(int commitCommitLogLeastPages) {
        this.commitCommitLogLeastPages = commitCommitLogLeastPages;
    }

    public int getFlushLeastPagesWhenWarmMapedFile() {
        return flushLeastPagesWhenWarmMapedFile;
    }

    public void setFlushLeastPagesWhenWarmMapedFile(int flushLeastPagesWhenWarmMapedFile) {
        this.flushLeastPagesWhenWarmMapedFile = flushLeastPagesWhenWarmMapedFile;
    }

    public int getFlushConsumeQueueLeastPages() {
        return flushConsumeQueueLeastPages;
    }

    public void setFlushConsumeQueueLeastPages(int flushConsumeQueueLeastPages) {
        this.flushConsumeQueueLeastPages = flushConsumeQueueLeastPages;
    }

    public int getFlushCommitLogThoroughInterval() {
        return flushCommitLogThoroughInterval;
    }

    public void setFlushCommitLogThoroughInterval(int flushCommitLogThoroughInterval) {
        this.flushCommitLogThoroughInterval = flushCommitLogThoroughInterval;
    }

    public int getCommitCommitLogThoroughInterval() {
        return commitCommitLogThoroughInterval;
    }

    public void setCommitCommitLogThoroughInterval(int commitCommitLogThoroughInterval) {
        this.commitCommitLogThoroughInterval = commitCommitLogThoroughInterval;
    }

    public int getFlushConsumeQueueThoroughInterval() {
        return flushConsumeQueueThoroughInterval;
    }

    public void setFlushConsumeQueueThoroughInterval(int flushConsumeQueueThoroughInterval) {
        this.flushConsumeQueueThoroughInterval = flushConsumeQueueThoroughInterval;
    }

    public int getMaxTransferBytesOnMessageInMemory() {
        return maxTransferBytesOnMessageInMemory;
    }

    public void setMaxTransferBytesOnMessageInMemory(int maxTransferBytesOnMessageInMemory) {
        this.maxTransferBytesOnMessageInMemory = maxTransferBytesOnMessageInMemory;
    }

    public int getMaxTransferCountOnMessageInMemory() {
        return maxTransferCountOnMessageInMemory;
    }

    public void setMaxTransferCountOnMessageInMemory(int maxTransferCountOnMessageInMemory) {
        this.maxTransferCountOnMessageInMemory = maxTransferCountOnMessageInMemory;
    }

    public int getMaxTransferBytesOnMessageInDisk() {
        return maxTransferBytesOnMessageInDisk;
    }

    public void setMaxTransferBytesOnMessageInDisk(int maxTransferBytesOnMessageInDisk) {
        this.maxTransferBytesOnMessageInDisk = maxTransferBytesOnMessageInDisk;
    }

    public int getMaxTransferCountOnMessageInDisk() {
        return maxTransferCountOnMessageInDisk;
    }

    public void setMaxTransferCountOnMessageInDisk(int maxTransferCountOnMessageInDisk) {
        this.maxTransferCountOnMessageInDisk = maxTransferCountOnMessageInDisk;
    }

    public int getAccessMessageInMemoryMaxRatio() {
        return accessMessageInMemoryMaxRatio;
    }

    public void setAccessMessageInMemoryMaxRatio(int accessMessageInMemoryMaxRatio) {
        this.accessMessageInMemoryMaxRatio = accessMessageInMemoryMaxRatio;
    }

    public boolean isMessageIndexEnable() {
        return messageIndexEnable;
    }

    public void setMessageIndexEnable(boolean messageIndexEnable) {
        this.messageIndexEnable = messageIndexEnable;
    }

    public int getMaxHashSlotNum() {
        return maxHashSlotNum;
    }

    public void setMaxHashSlotNum(int maxHashSlotNum) {
        this.maxHashSlotNum = maxHashSlotNum;
    }

    public int getMaxIndexNum() {
        return maxIndexNum;
    }

    public void setMaxIndexNum(int maxIndexNum) {
        this.maxIndexNum = maxIndexNum;
    }

    public int getMaxMsgsNumBatch() {
        return maxMsgsNumBatch;
    }

    public void setMaxMsgsNumBatch(int maxMsgsNumBatch) {
        this.maxMsgsNumBatch = maxMsgsNumBatch;
    }

    public boolean isMessageIndexSafe() {
        return messageIndexSafe;
    }

    public void setMessageIndexSafe(boolean messageIndexSafe) {
        this.messageIndexSafe = messageIndexSafe;
    }

    public int getHaListenPort() {
        return haListenPort;
    }

    public void setHaListenPort(int haListenPort) {
        this.haListenPort = haListenPort;
    }

    public int getHaSendHeartbeatInterval() {
        return haSendHeartbeatInterval;
    }

    public void setHaSendHeartbeatInterval(int haSendHeartbeatInterval) {
        this.haSendHeartbeatInterval = haSendHeartbeatInterval;
    }

    public int getHaHousekeepingInterval() {
        return haHousekeepingInterval;
    }

    public void setHaHousekeepingInterval(int haHousekeepingInterval) {
        this.haHousekeepingInterval = haHousekeepingInterval;
    }

    public int getHaTransferBatchSize() {
        return haTransferBatchSize;
    }

    public void setHaTransferBatchSize(int haTransferBatchSize) {
        this.haTransferBatchSize = haTransferBatchSize;
    }

    public String getHaMasterAddress() {
        return haMasterAddress;
    }

    public void setHaMasterAddress(String haMasterAddress) {
        this.haMasterAddress = haMasterAddress;
    }

    public int getHaMaxGapNotInSync() {
        return haMaxGapNotInSync;
    }

    public void setHaMaxGapNotInSync(int haMaxGapNotInSync) {
        this.haMaxGapNotInSync = haMaxGapNotInSync;
    }

    public BrokerRole getBrokerRole() {
        return brokerRole;
    }

    public void setBrokerRole(BrokerRole brokerRole) {
        this.brokerRole = brokerRole;
    }

    public FlushDiskType getFlushDiskType() {
        return flushDiskType;
    }

    public void setFlushDiskType(FlushDiskType flushDiskType) {
        this.flushDiskType = flushDiskType;
    }

    public int getSyncFlushTimeout() {
        return syncFlushTimeout;
    }

    public void setSyncFlushTimeout(int syncFlushTimeout) {
        this.syncFlushTimeout = syncFlushTimeout;
    }

    public int getPutMessageTimeout() {
        return putMessageTimeout;
    }

    public void setPutMessageTimeout(int putMessageTimeout) {
        this.putMessageTimeout = putMessageTimeout;
    }

    public int getSlaveTimeout() {
        return slaveTimeout;
    }

    public void setSlaveTimeout(int slaveTimeout) {
        this.slaveTimeout = slaveTimeout;
    }

    public String getMessageDelayLevel() {
        return messageDelayLevel;
    }

    public void setMessageDelayLevel(String messageDelayLevel) {
        this.messageDelayLevel = messageDelayLevel;
    }

    public long getFlushDelayOffsetInterval() {
        return flushDelayOffsetInterval;
    }

    public void setFlushDelayOffsetInterval(long flushDelayOffsetInterval) {
        this.flushDelayOffsetInterval = flushDelayOffsetInterval;
    }

    public boolean isCleanFileForciblyEnable() {
        return cleanFileForciblyEnable;
    }

    public void setCleanFileForciblyEnable(boolean cleanFileForciblyEnable) {
        this.cleanFileForciblyEnable = cleanFileForciblyEnable;
    }

    public boolean isWarmMapedFileEnable() {
        return warmMapedFileEnable;
    }

    public void setWarmMapedFileEnable(boolean warmMapedFileEnable) {
        this.warmMapedFileEnable = warmMapedFileEnable;
    }

    public boolean isOffsetCheckInSlave() {
        return offsetCheckInSlave;
    }

    public void setOffsetCheckInSlave(boolean offsetCheckInSlave) {
        this.offsetCheckInSlave = offsetCheckInSlave;
    }

    public boolean isDebugLockEnable() {
        return debugLockEnable;
    }

    public void setDebugLockEnable(boolean debugLockEnable) {
        this.debugLockEnable = debugLockEnable;
    }

    public boolean isDuplicationEnable() {
        return duplicationEnable;
    }

    public void setDuplicationEnable(boolean duplicationEnable) {
        this.duplicationEnable = duplicationEnable;
    }

    public boolean isDiskFallRecorded() {
        return diskFallRecorded;
    }

    public void setDiskFallRecorded(boolean diskFallRecorded) {
        this.diskFallRecorded = diskFallRecorded;
    }

    public long getOsPageCacheBusyTimeOutMills() {
        return osPageCacheBusyTimeOutMills;
    }

    public void setOsPageCacheBusyTimeOutMills(long osPageCacheBusyTimeOutMills) {
        this.osPageCacheBusyTimeOutMills = osPageCacheBusyTimeOutMills;
    }

    public int getDefaultQueryMaxNum() {
        return defaultQueryMaxNum;
    }

    public void setDefaultQueryMaxNum(int defaultQueryMaxNum) {
        this.defaultQueryMaxNum = defaultQueryMaxNum;
    }

    public boolean isTransientStorePoolEnable() {
        return transientStorePoolEnable;
    }

    public void setTransientStorePoolEnable(boolean transientStorePoolEnable) {
        this.transientStorePoolEnable = transientStorePoolEnable;
    }

    public int getTransientStorePoolSize() {
        return transientStorePoolSize;
    }

    public void setTransientStorePoolSize(int transientStorePoolSize) {
        this.transientStorePoolSize = transientStorePoolSize;
    }

    public boolean isFastFailIfNoBufferInStorePool() {
        return fastFailIfNoBufferInStorePool;
    }

    public void setFastFailIfNoBufferInStorePool(boolean fastFailIfNoBufferInStorePool) {
        this.fastFailIfNoBufferInStorePool = fastFailIfNoBufferInStorePool;
    }

    public boolean isEnableDLegerCommitLog() {
        return enableDLegerCommitLog;
    }

    public void setEnableDLegerCommitLog(boolean enableDLegerCommitLog) {
        this.enableDLegerCommitLog = enableDLegerCommitLog;
    }

    public String getdLegerGroup() {
        return dLegerGroup;
    }

    public void setdLegerGroup(String dLegerGroup) {
        this.dLegerGroup = dLegerGroup;
    }

    public String getdLegerPeers() {
        return dLegerPeers;
    }

    public void setdLegerPeers(String dLegerPeers) {
        this.dLegerPeers = dLegerPeers;
    }

    public String getdLegerSelfId() {
        return dLegerSelfId;
    }

    public void setdLegerSelfId(String dLegerSelfId) {
        this.dLegerSelfId = dLegerSelfId;
    }

    public String getPreferredLeaderId() {
        return preferredLeaderId;
    }

    public void setPreferredLeaderId(String preferredLeaderId) {
        this.preferredLeaderId = preferredLeaderId;
    }

    public boolean isEnableBatchPush() {
        return isEnableBatchPush;
    }

    public void setEnableBatchPush(boolean enableBatchPush) {
        isEnableBatchPush = enableBatchPush;
    }

    public boolean isEnableScheduleMessageStats() {
        return enableScheduleMessageStats;
    }

    public void setEnableScheduleMessageStats(boolean enableScheduleMessageStats) {
        this.enableScheduleMessageStats = enableScheduleMessageStats;
    }

    public boolean isEnableLmq() {
        return enableLmq;
    }

    public void setEnableLmq(boolean enableLmq) {
        this.enableLmq = enableLmq;
    }

    public boolean isEnableMultiDispatch() {
        return enableMultiDispatch;
    }

    public void setEnableMultiDispatch(boolean enableMultiDispatch) {
        this.enableMultiDispatch = enableMultiDispatch;
    }

    public int getMaxLmqConsumeQueueNum() {
        return maxLmqConsumeQueueNum;
    }

    public void setMaxLmqConsumeQueueNum(int maxLmqConsumeQueueNum) {
        this.maxLmqConsumeQueueNum = maxLmqConsumeQueueNum;
    }

    public boolean isEnableScheduleAsyncDeliver() {
        return enableScheduleAsyncDeliver;
    }

    public void setEnableScheduleAsyncDeliver(boolean enableScheduleAsyncDeliver) {
        this.enableScheduleAsyncDeliver = enableScheduleAsyncDeliver;
    }

    public int getScheduleAsyncDeliverMaxPendingLimit() {
        return scheduleAsyncDeliverMaxPendingLimit;
    }

    public void setScheduleAsyncDeliverMaxPendingLimit(int scheduleAsyncDeliverMaxPendingLimit) {
        this.scheduleAsyncDeliverMaxPendingLimit = scheduleAsyncDeliverMaxPendingLimit;
    }

    public int getScheduleAsyncDeliverMaxResendNum2Blocked() {
        return scheduleAsyncDeliverMaxResendNum2Blocked;
    }

    public void setScheduleAsyncDeliverMaxResendNum2Blocked(int scheduleAsyncDeliverMaxResendNum2Blocked) {
        this.scheduleAsyncDeliverMaxResendNum2Blocked = scheduleAsyncDeliverMaxResendNum2Blocked;
    }

    public int getMaxBatchDeleteFilesNum() {
        return maxBatchDeleteFilesNum;
    }

    public void setMaxBatchDeleteFilesNum(int maxBatchDeleteFilesNum) {
        this.maxBatchDeleteFilesNum = maxBatchDeleteFilesNum;
    }

    public int getDispatchCqThreads() {
        return dispatchCqThreads;
    }

    public void setDispatchCqThreads(int dispatchCqThreads) {
        this.dispatchCqThreads = dispatchCqThreads;
    }

    public int getDispatchCqCacheNum() {
        return dispatchCqCacheNum;
    }

    public void setDispatchCqCacheNum(int dispatchCqCacheNum) {
        this.dispatchCqCacheNum = dispatchCqCacheNum;
    }

    public boolean isEnableAsyncReput() {
        return enableAsyncReput;
    }

    public void setEnableAsyncReput(boolean enableAsyncReput) {
        this.enableAsyncReput = enableAsyncReput;
    }

    public boolean isRecheckReputOffsetFromCq() {
        return recheckReputOffsetFromCq;
    }

    public void setRecheckReputOffsetFromCq(boolean recheckReputOffsetFromCq) {
        this.recheckReputOffsetFromCq = recheckReputOffsetFromCq;
    }

    public int getMaxTopicLength() {
        return maxTopicLength;
    }

    public void setMaxTopicLength(int maxTopicLength) {
        this.maxTopicLength = maxTopicLength;
    }

    public int getTravelCqFileNumWhenGetMessage() {
        return travelCqFileNumWhenGetMessage;
    }

    public void setTravelCqFileNumWhenGetMessage(int travelCqFileNumWhenGetMessage) {
        this.travelCqFileNumWhenGetMessage = travelCqFileNumWhenGetMessage;
    }

    public int getCorrectLogicMinOffsetSleepInterval() {
        return correctLogicMinOffsetSleepInterval;
    }

    public void setCorrectLogicMinOffsetSleepInterval(int correctLogicMinOffsetSleepInterval) {
        this.correctLogicMinOffsetSleepInterval = correctLogicMinOffsetSleepInterval;
    }

    public int getCorrectLogicMinOffsetForceInterval() {
        return correctLogicMinOffsetForceInterval;
    }

    public void setCorrectLogicMinOffsetForceInterval(int correctLogicMinOffsetForceInterval) {
        this.correctLogicMinOffsetForceInterval = correctLogicMinOffsetForceInterval;
    }

    public boolean isMappedFileSwapEnable() {
        return mappedFileSwapEnable;
    }

    public void setMappedFileSwapEnable(boolean mappedFileSwapEnable) {
        this.mappedFileSwapEnable = mappedFileSwapEnable;
    }

    public long getCommitLogForceSwapMapInterval() {
        return commitLogForceSwapMapInterval;
    }

    public void setCommitLogForceSwapMapInterval(long commitLogForceSwapMapInterval) {
        this.commitLogForceSwapMapInterval = commitLogForceSwapMapInterval;
    }

    public long getCommitLogSwapMapInterval() {
        return commitLogSwapMapInterval;
    }

    public void setCommitLogSwapMapInterval(long commitLogSwapMapInterval) {
        this.commitLogSwapMapInterval = commitLogSwapMapInterval;
    }

    public int getCommitLogSwapMapReserveFileNum() {
        return commitLogSwapMapReserveFileNum;
    }

    public void setCommitLogSwapMapReserveFileNum(int commitLogSwapMapReserveFileNum) {
        this.commitLogSwapMapReserveFileNum = commitLogSwapMapReserveFileNum;
    }

    public long getLogicQueueForceSwapMapInterval() {
        return logicQueueForceSwapMapInterval;
    }

    public void setLogicQueueForceSwapMapInterval(long logicQueueForceSwapMapInterval) {
        this.logicQueueForceSwapMapInterval = logicQueueForceSwapMapInterval;
    }

    public long getLogicQueueSwapMapInterval() {
        return logicQueueSwapMapInterval;
    }

    public void setLogicQueueSwapMapInterval(long logicQueueSwapMapInterval) {
        this.logicQueueSwapMapInterval = logicQueueSwapMapInterval;
    }

    public long getCleanSwapedMapInterval() {
        return cleanSwapedMapInterval;
    }

    public void setCleanSwapedMapInterval(long cleanSwapedMapInterval) {
        this.cleanSwapedMapInterval = cleanSwapedMapInterval;
    }

    public int getLogicQueueSwapMapReserveFileNum() {
        return logicQueueSwapMapReserveFileNum;
    }

    public void setLogicQueueSwapMapReserveFileNum(int logicQueueSwapMapReserveFileNum) {
        this.logicQueueSwapMapReserveFileNum = logicQueueSwapMapReserveFileNum;
    }

    public boolean isSearchBcqByCacheEnable() {
        return searchBcqByCacheEnable;
    }

    public void setSearchBcqByCacheEnable(boolean searchBcqByCacheEnable) {
        this.searchBcqByCacheEnable = searchBcqByCacheEnable;
    }

    public boolean isDispatchFromSenderThread() {
        return dispatchFromSenderThread;
    }

    public void setDispatchFromSenderThread(boolean dispatchFromSenderThread) {
        this.dispatchFromSenderThread = dispatchFromSenderThread;
    }

    public boolean isWakeCommitWhenPutMessage() {
        return wakeCommitWhenPutMessage;
    }

    public void setWakeCommitWhenPutMessage(boolean wakeCommitWhenPutMessage) {
        this.wakeCommitWhenPutMessage = wakeCommitWhenPutMessage;
    }

    public boolean isWakeFlushWhenPutMessage() {
        return wakeFlushWhenPutMessage;
    }

    public void setWakeFlushWhenPutMessage(boolean wakeFlushWhenPutMessage) {
        this.wakeFlushWhenPutMessage = wakeFlushWhenPutMessage;
    }

    public boolean isEnableCleanExpiredOffset() {
        return enableCleanExpiredOffset;
    }

    public void setEnableCleanExpiredOffset(boolean enableCleanExpiredOffset) {
        this.enableCleanExpiredOffset = enableCleanExpiredOffset;
    }

    public int getMaxAsyncPutMessageRequests() {
        return maxAsyncPutMessageRequests;
    }

    public void setMaxAsyncPutMessageRequests(int maxAsyncPutMessageRequests) {
        this.maxAsyncPutMessageRequests = maxAsyncPutMessageRequests;
    }

    public int getPullBatchMaxMessageCount() {
        return pullBatchMaxMessageCount;
    }

    public void setPullBatchMaxMessageCount(int pullBatchMaxMessageCount) {
        this.pullBatchMaxMessageCount = pullBatchMaxMessageCount;
    }

    public int getTotalReplicas() {
        return totalReplicas;
    }

    public void setTotalReplicas(int totalReplicas) {
        this.totalReplicas = totalReplicas;
    }

    public int getInSyncReplicas() {
        return inSyncReplicas;
    }

    public void setInSyncReplicas(int inSyncReplicas) {
        this.inSyncReplicas = inSyncReplicas;
    }

    public int getMinInSyncReplicas() {
        return minInSyncReplicas;
    }

    public void setMinInSyncReplicas(int minInSyncReplicas) {
        this.minInSyncReplicas = minInSyncReplicas;
    }

    public boolean isAllAckInSyncStateSet() {
        return allAckInSyncStateSet;
    }

    public void setAllAckInSyncStateSet(boolean allAckInSyncStateSet) {
        this.allAckInSyncStateSet = allAckInSyncStateSet;
    }

    public boolean isEnableAutoInSyncReplicas() {
        return enableAutoInSyncReplicas;
    }

    public void setEnableAutoInSyncReplicas(boolean enableAutoInSyncReplicas) {
        this.enableAutoInSyncReplicas = enableAutoInSyncReplicas;
    }

    public boolean isHaFlowControlEnable() {
        return haFlowControlEnable;
    }

    public void setHaFlowControlEnable(boolean haFlowControlEnable) {
        this.haFlowControlEnable = haFlowControlEnable;
    }

    public long getMaxHaTransferByteInSecond() {
        return maxHaTransferByteInSecond;
    }

    public void setMaxHaTransferByteInSecond(long maxHaTransferByteInSecond) {
        this.maxHaTransferByteInSecond = maxHaTransferByteInSecond;
    }

    public long getHaMaxTimeSlaveNotCatchup() {
        return haMaxTimeSlaveNotCatchup;
    }

    public void setHaMaxTimeSlaveNotCatchup(long haMaxTimeSlaveNotCatchup) {
        this.haMaxTimeSlaveNotCatchup = haMaxTimeSlaveNotCatchup;
    }

    public boolean isSyncMasterFlushOffsetWhenStartup() {
        return syncMasterFlushOffsetWhenStartup;
    }

    public void setSyncMasterFlushOffsetWhenStartup(boolean syncMasterFlushOffsetWhenStartup) {
        this.syncMasterFlushOffsetWhenStartup = syncMasterFlushOffsetWhenStartup;
    }

    public long getMaxChecksumRange() {
        return maxChecksumRange;
    }

    public void setMaxChecksumRange(long maxChecksumRange) {
        this.maxChecksumRange = maxChecksumRange;
    }

    public int getReplicasPerDiskPartition() {
        return replicasPerDiskPartition;
    }

    public void setReplicasPerDiskPartition(int replicasPerDiskPartition) {
        this.replicasPerDiskPartition = replicasPerDiskPartition;
    }

    public double getLogicalDiskSpaceCleanForciblyThreshold() {
        return logicalDiskSpaceCleanForciblyThreshold;
    }

    public void setLogicalDiskSpaceCleanForciblyThreshold(double logicalDiskSpaceCleanForciblyThreshold) {
        this.logicalDiskSpaceCleanForciblyThreshold = logicalDiskSpaceCleanForciblyThreshold;
    }

    public long getMaxSlaveResendLength() {
        return maxSlaveResendLength;
    }

    public void setMaxSlaveResendLength(long maxSlaveResendLength) {
        this.maxSlaveResendLength = maxSlaveResendLength;
    }

    public boolean isSyncFromLastFile() {
        return syncFromLastFile;
    }

    public void setSyncFromLastFile(boolean syncFromLastFile) {
        this.syncFromLastFile = syncFromLastFile;
    }

    public boolean isAsyncLearner() {
        return asyncLearner;
    }

    public void setAsyncLearner(boolean asyncLearner) {
        this.asyncLearner = asyncLearner;
    }
}
