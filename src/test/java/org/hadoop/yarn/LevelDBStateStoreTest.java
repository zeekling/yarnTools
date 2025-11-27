package org.hadoop.yarn;

import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos;
import org.hadoop.yarn.state.RecoveredContainerState;
import org.junit.Test;

import java.util.List;

public class LevelDBStateStoreTest {


    private static LevelDBStateStore stateStore;

    private synchronized void initStateStore() {
        if (stateStore == null) {
            String path = LevelDBStateStore.class.getResource("/yarn-nm-state").getPath();
            System.out.println(path);
            stateStore = new LevelDBStateStore(path);
        }
    }

    @Test
    public void getAllContainersTest() {
        initStateStore();
        List<RecoveredContainerState> allContainers = stateStore.getAllContainers();
        for (RecoveredContainerState containerState : allContainers) {
            System.out.println(containerState);
        }
    }

    @Test
    public void getAllApplicationTest() {
        initStateStore();
        List<YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto> allApplications = stateStore.getAllApplications();
        for (YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto app : allApplications) {
            System.out.println(app.getId().getClusterTimestamp());
        }

    }


}
