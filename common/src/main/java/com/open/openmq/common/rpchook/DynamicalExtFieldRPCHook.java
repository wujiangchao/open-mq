
package com.open.openmq.common.rpchook;

import com.open.openmq.common.MixAll;
import com.open.openmq.remoting.RPCHook;
import com.open.openmq.remoting.protocol.RemotingCommand;
import org.apache.commons.lang3.StringUtils;

public class DynamicalExtFieldRPCHook implements RPCHook {

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        String zoneName = System.getProperty(MixAll.ROCKETMQ_ZONE_PROPERTY, System.getenv(MixAll.ROCKETMQ_ZONE_ENV));
        if (StringUtils.isNotBlank(zoneName)) {
            request.addExtField(MixAll.ZONE_NAME, zoneName);
        }
        String zoneMode = System.getProperty(MixAll.ROCKETMQ_ZONE_MODE_PROPERTY, System.getenv(MixAll.ROCKETMQ_ZONE_MODE_ENV));
        if (StringUtils.isNotBlank(zoneMode)) {
            request.addExtField(MixAll.ZONE_MODE, zoneMode);
        }
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
        
    }
}
