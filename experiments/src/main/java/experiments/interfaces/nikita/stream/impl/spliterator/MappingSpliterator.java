package experiments.interfaces.nikita.stream.impl.spliterator;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by marnikitta on 04.11.16.
 */
public class MappingSpliterator<S, R> extends Spliterators.AbstractSpliterator<R> implements Spliterator<R> {
  private final Spliterator<S> base;

  private final Function<? super S, ? extends R> mapepr;

  public MappingSpliterator(final Spliterator<S> base, final Function<? super S, ? extends R> mapper) {
    super(Long.MAX_VALUE, 0);
    this.base = base;
    this.mapepr = mapper;
  }

  @Override
  public boolean tryAdvance(final Consumer<? super R> action) {
    return base.tryAdvance(value -> action.accept(mapepr.apply(value)));
  }
}
