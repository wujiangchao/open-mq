package com.open.openmq.remoting.exception;

/**
 * connect exception
 * 例如 NettyRemotingClient在channel出现问题的时候会抛出RemotingConnectException
 *
 * @author 34246
 */
public class RemotingConnectException extends RemotingException {
    private static final long serialVersionUID = -5565366231695911316L;

    public RemotingConnectException(String addr) {
        this(addr, null);
    }

    public RemotingConnectException(String addr, Throwable cause) {
        super("connect to " + addr + " failed", cause);
    }
}
