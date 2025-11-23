package org.hadoop.yarn.state;

public enum RecoveredContainerStatus {
    REQUESTED,
    QUEUED,
    LAUNCHED,
    COMPLETED,
    PAUSED
}
