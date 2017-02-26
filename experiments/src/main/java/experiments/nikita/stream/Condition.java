package experiments.nikita.stream;

/**
 * Created by marnikitta on 19.10.16.
 */
public interface Condition<S> {
  RunningCondition<S> instance();
}
