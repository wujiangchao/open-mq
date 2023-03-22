package com.open.openmq.remoting.protocol;

/**
 * @Description TODO
 * @Date 2023/2/21 20:54
 * @Author jack wu
 */
public class RemotingCommand {
    public static final String REMOTING_VERSION_KEY = "openmq.remoting.version";

    private int code;


    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }
}
