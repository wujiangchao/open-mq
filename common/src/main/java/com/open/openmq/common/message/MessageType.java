package com.open.openmq.common.message;

public enum MessageType {
    //普通消息
    Normal_Msg("Normal"),
    //顺序消息
    Order_Msg("Order"),
    //事务-预提交消息
    Trans_Msg_Half("Trans"),
    //事务-提交消息
    Trans_msg_Commit("TransCommit"),
    //定时、延迟消息
    Delay_Msg("Delay");

    private final String shortName;

    MessageType(String shortName) {
        this.shortName = shortName;
    }

    public String getShortName() {
        return shortName;
    }

    public static MessageType getByShortName(String shortName) {
        for (MessageType msgType : MessageType.values()) {
            if (msgType.getShortName().equals(shortName)) {
                return msgType;
            }
        }
        return Normal_Msg;
    }
}
