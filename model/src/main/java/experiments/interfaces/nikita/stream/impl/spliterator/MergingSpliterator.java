package experiments.interfaces.nikita.stream.impl.spliterator;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;

/**
 * Created by marnikitta on 02.11.16.
 */
public class MergingSpliterator<T extends Comparable<T>> extends Spliterators.AbstractSpliterator<T> implements Spliterator<T> {
    private final Spliterator<T> spliterator1;
    private final Spliterator<T> spliterator2;

    private T s1;
    private T s2;

    public MergingSpliterator(final Spliterator<T> spliterator1, final Spliterator<T> spliterator2) {
        super(Long.MAX_VALUE, spliterator1.characteristics() & spliterator2.characteristics());

        this.spliterator1 = spliterator1;
        this.spliterator2 = spliterator2;
    }

    @Override
    public boolean tryAdvance(final Consumer<? super T> action) {
        if (s1 == null) {
            spliterator1.tryAdvance(el -> s1 = el);
        }

        if (s2 == null) {
            spliterator2.tryAdvance(el -> s2 = el);
        }

        if (s1 == null && s2 == null) {
            return false;
        } else if (s1 == null) {
            action.accept(s2);
            s2 = null;
            return true;
        } else if (s2 == null) {
            action.accept(s1);
            s1 = null;
            return true;
        } else {
            final int com = s1.compareTo(s2);
            if (com < 0) {
                action.accept(s1);
                s1 = null;
                return true;
            } else {
                action.accept(s2);
                s2 = null;
                return true;
            }
        }
    }
}
