package experiments.interfaces.nikita.fake;

import experiments.interfaces.nikita.Condition;
import experiments.interfaces.nikita.Type;

import java.util.Collections;
import java.util.Set;

/**
 * Created by marnikitta on 19.10.16.
 */
public class OddNumber implements Type<Integer> {
    @Override
    public String name() {
        return "oddNumber";
    }

    private final long maxFails;

    public OddNumber(final long maxFails) {
        this.maxFails = maxFails;
    }

    @Override
    public Set<Condition<Integer>> conditions() {
        return Collections.singleton(new OddCondition(maxFails));
    }
}
