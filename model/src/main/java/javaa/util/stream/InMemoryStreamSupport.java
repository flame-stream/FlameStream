package javaa.util.stream;

import experiments.interfaces.nikita.DataItem;
import experiments.interfaces.nikita.Type;
import experiments.interfaces.nikita.YetAnotherStream;

import java.util.Objects;
import java.util.Spliterator;
import javaa.util.stream.StreamOpFlag;

/**
 * Created by marnikitta on 02.11.16.
 */
public class InMemoryStreamSupport {
    public static <S> YetAnotherStream<S> stream(final Spliterator<DataItem<S>> spliterator, final Type<S> type) {
        Objects.requireNonNull(spliterator);
        return new InMemoryStream.Head<>(spliterator,
                StreamOpFlag.fromCharacteristics(spliterator),
                false, type
        );
    }
}
