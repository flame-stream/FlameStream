package experiments.interfaces.nikita.stream.impl.spliterator;

import experiments.interfaces.nikita.stream.Grouping;

import java.util.*;
import java.util.function.Consumer;

/**
 * Created by marnikitta on 04.11.16.
 */
public class GroupingSpliterator<S> extends Spliterators.AbstractSpliterator<List<S>> implements Spliterator<List<S>> {
  private final Spliterator<S> base;

  private final int window;

  private final Grouping<S> grouping;

  private final Map<Integer, List<S>> groupingState = new HashMap<>();

  public GroupingSpliterator(final Spliterator<S> base, final Grouping<S> grouping, final int window) {
    super(Long.MAX_VALUE, 0);
    this.base = base;
    this.grouping = grouping;
    this.window = window;
  }

  private S next = null;

  @Override
  public boolean tryAdvance(final Consumer<? super List<S>> action) {
    if (!base.tryAdvance(s -> next = s)) {
      return false;
    } else {
      final int hash = grouping.applyAsInt(next);

      final List<S> hashList = groupingState.getOrDefault(hash, new ArrayList<>());
      hashList.add(next);
      final List<S> result = new ArrayList<>(hashList.subList(Math.max(hashList.size() - window, 0), hashList.size()));

      groupingState.put(hash, result);

      action.accept(result);
      return true;
    }
  }
}
