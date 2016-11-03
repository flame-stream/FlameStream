package experiments.interfaces.nikita;

import java.util.Comparator;

/**
 * Created by marnikitta on 19.10.16.
 */
public interface Meta extends Comparable<Meta> {
    Meta incremented();

    Comparator<Meta> comparator();
}
