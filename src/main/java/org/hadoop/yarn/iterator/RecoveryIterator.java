package org.hadoop.yarn.iterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;

public interface RecoveryIterator<T> extends Closeable {

    /**
     * Returns true if the iteration has more elements.
     */
    boolean hasNext() throws IOException;

    /**
     * Returns the next element in the iteration.
     */
    T next() throws IOException, NoSuchElementException;

}
