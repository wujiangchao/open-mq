package com.open.openmq.namesrv.routeinfo;

import com.open.openmq.common.ServiceThread;

/**
 * @Description TODO
 * @Date 2023/3/3 14:45
 * @Author jack wu
 */
public class BatchUnregistrationService extends ServiceThread {
    private final RouteInfoManager routeInfoManager;
    private BlockingQueue<UnRegisterBrokerRequestHeader> unregistrationQueue;
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    @Override
    public void run() {

    }

    @Override
    public String getServiceName() {
        return BatchUnregistrationService.class.getName();
    }

}
