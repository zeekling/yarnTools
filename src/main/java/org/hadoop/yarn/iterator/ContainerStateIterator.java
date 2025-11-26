package org.hadoop.yarn.iterator;

import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.hadoop.yarn.state.RecoveredContainerState;
import org.hadoop.yarn.state.RecoveredContainerStatus;
import org.hadoop.yarn.state.RecoveredContainerType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.hadoop.yarn.LevelDBStateStore.*;

public class ContainerStateIterator extends
        BaseRecoveryIterator<RecoveredContainerState>  {

    public ContainerStateIterator(DB db) throws IOException {
        super(CONTAINERS_KEY_PREFIX, db);
    }

    @Override
    protected RecoveredContainerState getNextItem(LeveldbIterator it)
            throws IOException {
        return getNextRecoveredContainer(it);
    }

    private RecoveredContainerState getNextRecoveredContainer(LeveldbIterator it)
            throws IOException {
        RecoveredContainerState rcs = null;
        try {
            while (it.hasNext()) {
                Map.Entry<byte[], byte[]> entry = it.peekNext();
                String key = asString(entry.getKey());
                if (!key.startsWith(CONTAINERS_KEY_PREFIX)) {
                    return null;
                }

                int idEndPos = key.indexOf('/', CONTAINERS_KEY_PREFIX.length());
                if (idEndPos < 0) {
                    throw new IOException("Unable to determine container in key: " + key);
                }
                String keyPrefix = key.substring(0, idEndPos + 1);
                rcs = loadContainerState(it, keyPrefix);
                if (rcs.getStartRequest() != null) {
                    break;
                } else {
                    rcs = null;
                }
            }
        } catch (DBException e) {
            throw new IOException(e);
        }
        return rcs;
    }

    private RecoveredContainerState loadContainerState(LeveldbIterator iter,
                                                       String keyPrefix) throws IOException {
        ContainerId containerId = ContainerId.fromString(
                keyPrefix.substring(CONTAINERS_KEY_PREFIX.length(),
                        keyPrefix.length()-1));
        RecoveredContainerState rcs = new RecoveredContainerState(containerId);
        rcs.setStatus(RecoveredContainerStatus.REQUESTED);
        while (iter.hasNext()) {
            Map.Entry<byte[],byte[]> entry = iter.peekNext();
            String key = asString(entry.getKey());
            if (!key.startsWith(keyPrefix)) {
                break;
            }
            iter.next();

            String suffix = key.substring(keyPrefix.length()-1);  // start with '/'
            if (suffix.equals(CONTAINER_REQUEST_KEY_SUFFIX)) {
                rcs.setStartRequest(new StartContainerRequestPBImpl(
                        YarnServiceProtos.StartContainerRequestProto.parseFrom(entry.getValue())));
                ContainerTokenIdentifier containerTokenIdentifier = BuilderUtils
                        .newContainerTokenIdentifier(rcs.getStartRequest().getContainerToken());
                rcs.setCapability(new ResourcePBImpl(containerTokenIdentifier.getProto().getResource()));
            } else if (suffix.equals(CONTAINER_VERSION_KEY_SUFFIX)) {
                rcs.setVersion(Integer.parseInt(asString(entry.getValue())));
            } else if (suffix.equals(CONTAINER_START_TIME_KEY_SUFFIX)) {
                rcs.setStartTime(Long.parseLong(asString(entry.getValue())));
            } else if (suffix.equals(CONTAINER_QUEUED_KEY_SUFFIX)) {
                if (rcs.getStatus() == RecoveredContainerStatus.REQUESTED) {
                    rcs.setStatus(RecoveredContainerStatus.QUEUED);
                }

            } else if (suffix.equals(CONTAINER_PAUSED_KEY_SUFFIX)) {
                if ((rcs.getStatus() == RecoveredContainerStatus.LAUNCHED)
                        ||(rcs.getStatus() == RecoveredContainerStatus.QUEUED)
                        ||(rcs.getStatus() == RecoveredContainerStatus.REQUESTED)) {
                    rcs.setStatus(RecoveredContainerStatus.PAUSED);
                }
            } else if (suffix.equals(CONTAINER_LAUNCHED_KEY_SUFFIX)) {
                if ((rcs.getStatus() == RecoveredContainerStatus.REQUESTED)
                        || (rcs.getStatus() == RecoveredContainerStatus.QUEUED)
                        ||(rcs.getStatus() == RecoveredContainerStatus.PAUSED)) {
                    rcs.setStatus(RecoveredContainerStatus.LAUNCHED);
                }
            } else if (suffix.equals(CONTAINER_KILLED_KEY_SUFFIX)) {
                rcs.setKilled(true);
            } else if (suffix.equals(CONTAINER_EXIT_CODE_KEY_SUFFIX)) {
                rcs.setStatus(RecoveredContainerStatus.COMPLETED);
                rcs.setExitCode(Integer.parseInt(asString(entry.getValue())));
            } else if (suffix.equals(CONTAINER_UPDATE_TOKEN_SUFFIX)) {
                YarnSecurityTokenProtos.ContainerTokenIdentifierProto tokenIdentifierProto =
                        YarnSecurityTokenProtos.ContainerTokenIdentifierProto.parseFrom(entry.getValue());
                Token currentToken = rcs.getStartRequest().getContainerToken();
                rcs.setCapability(new ResourcePBImpl(tokenIdentifierProto.getResource()));
                rcs.setVersion(tokenIdentifierProto.getVersion());
            } else if (suffix.equals(CONTAINER_REMAIN_RETRIES_KEY_SUFFIX)) {
                rcs.setRemainingRetryAttempts(
                        Integer.parseInt(asString(entry.getValue())));
            } else if (suffix.equals(CONTAINER_RESTART_TIMES_SUFFIX)) {
                String value = asString(entry.getValue());
                // parse the string format of List<Long>, e.g. [34, 21, 22]
                String[] unparsedRestartTimes =
                        value.substring(1, value.length() - 1).split(", ");
                List<Long> restartTimes = new ArrayList<>();
                for (String restartTime : unparsedRestartTimes) {
                    if (!restartTime.isEmpty()) {
                        restartTimes.add(Long.parseLong(restartTime));
                    }
                }
                rcs.setRestartTimes(restartTimes);
            } else if (suffix.equals(CONTAINER_WORK_DIR_KEY_SUFFIX)) {
                rcs.setWorkDir(asString(entry.getValue()));
            } else if (suffix.equals(CONTAINER_LOG_DIR_KEY_SUFFIX)) {
                rcs.setLogDir(asString(entry.getValue()));
            } else if (suffix.startsWith(CONTAINER_ASSIGNED_RESOURCES_KEY_SUFFIX)) {
                // skip
            } else {
                System.out.println("the container " + containerId
                        + " will be killed because of the unknown key " + key
                        + " during recovery.");
                rcs.setRecoveryType(RecoveredContainerType.KILL);
            }
        }
        return rcs;
    }
}
