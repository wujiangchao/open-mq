
package com.open.openmq.remoting.exception;

/**
 * NettyRemotingAbstract在执行请求之前要进行流控，如果获取不到信号量，则区分是否是超时，
 * 如果是无timeout获取信号量也没获取到，则表示RemotingTooMuchRequestException
 *
 * @author 34246
 */
public class RemotingTooMuchRequestException extends RemotingException {
    private static final long serialVersionUID = 4326919581254519654L;

    public RemotingTooMuchRequestException(String message) {
        super(message);
    }
}
