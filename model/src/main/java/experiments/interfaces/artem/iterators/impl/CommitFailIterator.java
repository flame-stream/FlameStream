package experiments.interfaces.artem.iterators.impl;

import java.util.Iterator;

public interface CommitFailIterator<T> extends Iterator<T> {
    void commit();
    void fail();
}
