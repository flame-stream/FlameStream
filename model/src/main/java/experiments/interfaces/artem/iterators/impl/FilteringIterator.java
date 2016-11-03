package experiments.interfaces.artem.iterators.impl;

import experiments.interfaces.artem.mockstream.DataItem;

import java.util.Iterator;
import java.util.function.Function;

public class FilteringIterator<T extends DataItem, R extends DataItem> implements Iterator<R> {
    private final Function<T, R> filter;
    private final Iterator<T> iterator;

    public FilteringIterator(Iterator<T> iterator, Function<T, R> filter) {
        this.iterator = iterator;
        this.filter = filter;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public R next() {
        T next = iterator.next();
        return filter.apply(next);
    }
}
