package experiments.nikita.stream;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Created by marnikitta on 12.10.16.
 */
public interface YetAnotherStream<S> {
  Type<S> type();

  boolean isValid();

  Stream<S> materialize();

  YetAnotherStream<S> split();

  <R> YetAnotherStream<R> map(Function<? super S, ? extends R> mapper, Type<R> targetType);

  YetAnotherStream<S> mergeWith(YetAnotherStream<S> that);

  YetAnotherStream<S> mergeWith(Supplier<YetAnotherStream<S>> that);

  YetAnotherStream<List<S>> groupBy(Grouping<S> grouping, int window);
}
