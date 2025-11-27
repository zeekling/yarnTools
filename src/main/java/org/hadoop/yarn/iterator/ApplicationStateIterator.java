package org.hadoop.yarn.iterator;

import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;

import java.io.IOException;
import java.util.Map;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.hadoop.yarn.LevelDBStateStore.APPLICATIONS_KEY_PREFIX;

public class ApplicationStateIterator extends BaseRecoveryIterator<YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto> {

    public ApplicationStateIterator(DB db) throws IOException {
        super(APPLICATIONS_KEY_PREFIX, db);
    }

    @Override
    protected YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto getNextItem(LeveldbIterator it)
            throws IOException {
        return getNextRecoveredApplication(it);
    }

    private YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto getNextRecoveredApplication(LeveldbIterator it) throws IOException {
        YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto applicationProto = null;
        try {
            while (it.hasNext()) {
                Map.Entry<byte[], byte[]> entry = it.next();
                String key = asString(entry.getKey());
                if (!key.startsWith(APPLICATIONS_KEY_PREFIX)) {
                    continue;
                }
                applicationProto = YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto.parseFrom(
                        entry.getValue());
            }
        } catch (DBException e) {
            throw new IOException(e);
        }
        return applicationProto;
    }

}
