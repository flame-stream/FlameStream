package experiments.nikita.stream.impl.spliterator;

import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Created by marnikitta on 04.11.16.
 */
public interface PeekingSpliterator<T> extends Spliterator<T> {
  boolean tryPeekAdvance(Consumer<? super T> action);
}
