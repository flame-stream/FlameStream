package experiments.interfaces.nikita;

import java.io.Serializable;

/**
 * Created by marnikitta on 19.10.16.
 */
public interface Condition<S> {
    RunningCondition<S> instance();
}
