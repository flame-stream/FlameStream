package experiments.interfaces.nikita;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by marnikitta on 19.10.16.
 */

/**
 * @param <S> support of the type
 */
public interface Type<S> {
    String name();

    Set<Condition<S>> conditions();
}
