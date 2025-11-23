package org.hadoop.yarn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.fusesource.leveldbjni.JniDBFactory;
import org.hadoop.yarn.iterator.ContainerStateIterator;
import org.hadoop.yarn.state.RecoveredContainerState;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LevelDBStateStore {

    public static final String CONTAINERS_KEY_PREFIX = "ContainerManager/containers/";
    public static final String LOCALIZATION_STARTED_SUFFIX = "started/";
    public static final String LOCALIZATION_COMPLETED_SUFFIX = "completed/";
    public static final String LOCALIZATION_FILECACHE_SUFFIX = "filecache/";
    public static final String LOCALIZATION_APPCACHE_SUFFIX = "appcache/";

    public static final String CONTAINER_REQUEST_KEY_SUFFIX = "/request";
    public static final String CONTAINER_VERSION_KEY_SUFFIX = "/version";
    public static final String CONTAINER_START_TIME_KEY_SUFFIX = "/starttime";
    public static final String CONTAINER_DIAGS_KEY_SUFFIX = "/diagnostics";
    public static final String CONTAINER_LAUNCHED_KEY_SUFFIX = "/launched";
    public static final String CONTAINER_QUEUED_KEY_SUFFIX = "/queued";
    public static final String CONTAINER_PAUSED_KEY_SUFFIX = "/paused";
    public static final String CONTAINER_UPDATE_TOKEN_SUFFIX = "/updateToken";
    public static final String CONTAINER_KILLED_KEY_SUFFIX = "/killed";
    public static final String CONTAINER_EXIT_CODE_KEY_SUFFIX = "/exitcode";
    public static final String CONTAINER_REMAIN_RETRIES_KEY_SUFFIX = "/remainingRetryAttempts";
    public static final String CONTAINER_RESTART_TIMES_SUFFIX = "/restartTimes";
    public static final String CONTAINER_WORK_DIR_KEY_SUFFIX = "/workdir";
    public static final String CONTAINER_LOG_DIR_KEY_SUFFIX = "/logdir";
    public static final String CONTAINER_ASSIGNED_RESOURCES_KEY_SUFFIX = "/assignedResources_";

    public static final String CURRENT_MASTER_KEY_SUFFIX = "CurrentMasterKey";
    public static final String PREV_MASTER_KEY_SUFFIX = "PreviousMasterKey";
    public static final String NEXT_MASTER_KEY_SUFFIX = "NextMasterKey";
    public static final String NM_TOKENS_KEY_PREFIX = "NMTokens/";

    public static final Logger LOG = LoggerFactory.getLogger(LevelDBStateStore.class);

    private final DB db;

    public LevelDBStateStore(String storeRoot) {
        Options options = new Options();
        options.createIfMissing(false);
        File dbfile = new File(storeRoot);
        try {
            db = JniDBFactory.factory.open(dbfile, options);
        }  catch (IOException e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    public List<RecoveredContainerState> getAllContainers() {
        List<RecoveredContainerState> containerStates = new ArrayList<RecoveredContainerState>();
        try (ContainerStateIterator rcsIterator = new ContainerStateIterator(db)) {
            while (rcsIterator.hasNext()) {
                RecoveredContainerState rcs = rcsIterator.next();
                LOG.debug("Recovering container with state: {}", rcs);
                containerStates.add(rcs);
            }
        } catch (IOException e) {
            LOG.error(e.toString());
        }
        return containerStates;
    }


}
