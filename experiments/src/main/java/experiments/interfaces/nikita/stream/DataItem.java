package experiments.interfaces.nikita.stream;

import java.util.function.Function;

/**
 * Created by marnikitta on 02.11.16.
 */
public class DataItem<S> implements Comparable<DataItem<S>> {
    private final S value;

    private final Meta meta;

    public DataItem(final S value, final Meta meta) {
        this.value = value;
        this.meta = meta;
    }

    public S value() {
        return this.value;
    }

    public Meta meta() {
        return this.meta;
    }

    public DataItem<S> incremented() {
        return new DataItem<>(value(), meta().incremented());
    }

    public <R> DataItem<R> map(Function<? super S, ? extends R> function) {
        return new DataItem<>(function.apply(value()), meta());
    }

    @Override
    public int compareTo(final DataItem<S> o) {
        return this.meta().compareTo(o.meta());
    }
}
