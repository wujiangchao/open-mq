
package com.open.openmq.remoting.exception;

/**
 * NettyRemotingClient在发送请求失败的时候，会抛出RemotingSendRequestException
 * @author 34246
 */
public class RemotingSendRequestException extends RemotingException {
    private static final long serialVersionUID = 5391285827332471674L;

    public RemotingSendRequestException(String addr) {
        this(addr, null);
    }

    public RemotingSendRequestException(String addr, Throwable cause) {
        super("send request to <" + addr + "> failed", cause);
    }
}
