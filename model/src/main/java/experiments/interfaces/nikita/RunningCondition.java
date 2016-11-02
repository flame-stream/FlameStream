package experiments.interfaces.nikita;

/**
 * Created by marnikitta on 19.10.16.
 */
public interface RunningCondition<S> {
    void update(S value);

    /**
     * Once invalid
     * Always invalid
     */
    boolean isValid();
}
