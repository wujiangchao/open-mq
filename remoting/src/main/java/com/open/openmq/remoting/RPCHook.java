
package com.open.openmq.remoting;


import com.open.openmq.remoting.protocol.RemotingCommand;

public interface RPCHook {
    void doBeforeRequest(final String remoteAddr, final RemotingCommand request);

    void doAfterResponse(final String remoteAddr, final RemotingCommand request,
                         final RemotingCommand response);
}
