package experiments.interfaces.nikita.impl;

import experiments.interfaces.nikita.DataItem;
import experiments.interfaces.nikita.Meta;

import java.util.function.Function;

/**
 * Created by marnikitta on 02.11.16.
 */
public class SimpleDataItem<S> implements DataItem<S> {
    private final S value;

    private final Meta meta;

    public SimpleDataItem(final S value, final Meta meta) {
        this.value = value;
        this.meta = meta;
    }

    @Override
    public S value() {
        return this.value;
    }

    @Override
    public Meta meta() {
        return this.meta;
    }

    @Override
    public DataItem<S> incremented() {
        return new SimpleDataItem<>(this.value, this.meta.incremented());
    }

    @Override
    public <R> DataItem<R> map(final Function<? super S, ? extends R> function) {
        return new SimpleDataItem<R>(function.apply(this.value), this.meta);
    }

    @Override
    public int compareTo(final DataItem<S> o) {
        return this.meta.compareTo(o.meta());
    }
}
