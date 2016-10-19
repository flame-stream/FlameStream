package experiments.interfaces.nikita;

import java.io.Serializable;

/**
 * Created by marnikitta on 19.10.16.
 */
public interface RunningCondition<S> extends Serializable {
    void update(S value);

    /**
     * Once invalid
     * Always invalid
     */
    boolean isValid();
}
