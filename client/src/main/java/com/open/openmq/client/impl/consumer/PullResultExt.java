package com.open.openmq.client.impl.consumer;

import com.open.openmq.client.consumer.PullResult;
import com.open.openmq.client.consumer.PullStatus;
import com.open.openmq.common.message.MessageExt;

import java.util.List;

/**
 * @Description 继承自PullResult类。它提供了额外的属性和方法，用于处理拉消息的结果。
 * @Date 2023/6/6 17:38
 * @Author jack wu
 */
public class PullResultExt extends PullResult {
    private final long suggestWhichBrokerId;
    private byte[] messageBinary;

    private final Long offsetDelta;

    public PullResultExt(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset,
                         List<MessageExt> msgFoundList, final long suggestWhichBrokerId, final byte[] messageBinary) {
        this(pullStatus, nextBeginOffset, minOffset, maxOffset, msgFoundList, suggestWhichBrokerId, messageBinary, 0L);
    }

    public PullResultExt(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset,
                         List<MessageExt> msgFoundList, final long suggestWhichBrokerId, final byte[] messageBinary, final Long offsetDelta) {
        super(pullStatus, nextBeginOffset, minOffset, maxOffset, msgFoundList);
        this.suggestWhichBrokerId = suggestWhichBrokerId;
        this.messageBinary = messageBinary;
        this.offsetDelta = offsetDelta;
    }

    public Long getOffsetDelta() {
        return offsetDelta;
    }

    public byte[] getMessageBinary() {
        return messageBinary;
    }

    public void setMessageBinary(byte[] messageBinary) {
        this.messageBinary = messageBinary;
    }

    public long getSuggestWhichBrokerId() {
        return suggestWhichBrokerId;
    }

}
