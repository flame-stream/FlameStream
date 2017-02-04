package experiments.interfaces.nikita.stream;

import java.util.Collections;
import java.util.Set;

/**
 * Created by marnikitta on 02.11.16.
 */
public class EmptyType<S> implements Type<S> {
  @Override
  public String name() {
    return "empty-type";
  }

  @Override
  public Set<Condition<S>> conditions() {
    return Collections.emptySet();
  }
}
