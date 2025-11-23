package org.hadoop.yarn.iterator;

import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;

import java.io.IOException;
import java.util.NoSuchElementException;

public abstract class BaseRecoveryIterator<T> implements RecoveryIterator<T> {
    protected LeveldbIterator it;
    T nextItem;

    BaseRecoveryIterator(String dbKey, DB db) throws IOException {
        this.it = getLevelDBIterator(dbKey, db);
        this.nextItem = null;
    }

    protected abstract T getNextItem(LeveldbIterator it) throws IOException;

    private LeveldbIterator getLevelDBIterator(String startKey, DB db)
            throws IOException {
        try {
            LeveldbIterator it = new LeveldbIterator(db);
            it.seek(JniDBFactory.bytes(startKey));
            return it;
        } catch (DBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (nextItem == null) {
            nextItem = getNextItem(it);
        }
        return (nextItem != null);
    }

    @Override
    public T next() throws IOException, NoSuchElementException {
        T tmp = nextItem;
        if (tmp != null) {
            nextItem = null;
            return tmp;
        } else {
            tmp = getNextItem(it);
            if (tmp == null) {
                throw new NoSuchElementException();
            }
            return tmp;
        }
    }

    @Override
    public void close() throws IOException {
        if (it != null) {
            it.close();
        }
    }

}
