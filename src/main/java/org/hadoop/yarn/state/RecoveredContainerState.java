package org.hadoop.yarn.state;


import lombok.Data;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.List;

@Data
public class RecoveredContainerState {
    private RecoveredContainerStatus status;
    private int exitCode = ContainerExitStatus.INVALID;
    private boolean killed = false;
    private String diagnostics = "";
    private StartContainerRequest startRequest;
    private Resource capability;
    private int remainingRetryAttempts = ContainerRetryContext.RETRY_INVALID;
    private List<Long> restartTimes;
    private String workDir;
    private String logDir;
    private int version;
    private RecoveredContainerType recoveryType = RecoveredContainerType.RECOVER;
    private long startTime;
    private final ContainerId containerId;

    public RecoveredContainerState(ContainerId containerId){
        this.containerId = containerId;
    }

    public boolean getKilled() {
        return killed;
    }

    @Override
    public String toString() {
        return new StringBuilder("Status: ").append(getStatus())
                .append(", Exit code: ").append(exitCode)
                .append(", Version: ").append(version)
                .append(", Start Time: ").append(startTime)
                .append(", Killed: ").append(getKilled())
                .append(", Diagnostics: ").append(getDiagnostics())
                .append(", Capability: ").append(getCapability())
                .append(", StartRequest: ").append(getStartRequest())
                .append(", RemainingRetryAttempts: ").append(remainingRetryAttempts)
                .append(", RestartTimes: ").append(restartTimes)
                .append(", WorkDir: ").append(workDir)
                .append(", LogDir: ").append(logDir)
                .toString();
    }

}
