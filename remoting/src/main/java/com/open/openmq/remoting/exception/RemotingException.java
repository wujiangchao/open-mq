package com.open.openmq.remoting.exception;

/**
 * Remoting 根异常
 *
 * @author 34246
 */
public class RemotingException extends Exception {
    private static final long serialVersionUID = -5690687334570505110L;

    public RemotingException(String message) {
        super(message);
    }

    public RemotingException(String message, Throwable cause) {
        super(message, cause);
    }
}
