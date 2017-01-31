package experiments.interfaces.artem.iterators;

import java.util.Iterator;

public interface CommitFailIterator<T> extends Iterator<T> {
    void commit();
    void fail();
}
