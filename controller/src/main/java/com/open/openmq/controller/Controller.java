package com.open.openmq.controller;

import com.open.openmq.common.protocol.controller.ElectMasterRequestHeader;
import com.open.openmq.remoting.RemotingServer;
import com.open.openmq.remoting.protocol.RemotingCommand;

import java.util.concurrent.CompletableFuture;

/**
 * @Description TODO
 * @Date 2023/5/17 13:44
 * @Author jack wu
 */
public interface Controller {
    /**
     * Startup controller
     */
    void startup();

    /**
     * Shutdown controller
     */
    void shutdown();

    /**
     * Start scheduling controller events, this function only will be triggered when the controller becomes leader.
     */
    void startScheduling();

    /**
     * Stop scheduling controller events, this function only will be triggered when the controller lose leadership.
     */
    void stopScheduling();

    /**
     * Whether this controller is in leader state.
     */
    boolean isLeaderState();

    /**
     * Elect new master for a broker.
     *
     * @param request ElectMasterRequest
     * @return RemotingCommand(ElectMasterResponseHeader)
     */
    CompletableFuture<RemotingCommand> electMaster(final ElectMasterRequestHeader request);

    /**
     * Get the remotingServer used by the controller, the upper layer will reuse this remotingServer.
     */
    RemotingServer getRemotingServer();
}
