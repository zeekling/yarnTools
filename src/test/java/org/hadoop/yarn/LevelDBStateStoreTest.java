package org.hadoop.yarn;

import org.hadoop.yarn.state.RecoveredContainerState;
import org.junit.Test;

import java.util.List;

public class LevelDBStateStoreTest {

    @Test
    public void getAllContainersTest() {
        String path = LevelDBStateStore.class.getResource("/yarn-nm-state").getPath();
        System.out.println(path);
        LevelDBStateStore stateStore = new LevelDBStateStore(path);
        List<RecoveredContainerState> allContainers = stateStore.getAllContainers();
        for (RecoveredContainerState containerState : allContainers) {
            System.out.println(containerState);
        }
    }

}
