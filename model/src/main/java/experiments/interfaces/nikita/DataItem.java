package experiments.interfaces.nikita;

import java.util.function.Function;

/**
 * Created by marnikitta on 02.11.16.
 */
public interface DataItem<S> {
    S value();

    Meta meta();

    DataItem<S> incremented();

    <R> DataItem<R> map(Function<? super S, ? extends R> function);
}
