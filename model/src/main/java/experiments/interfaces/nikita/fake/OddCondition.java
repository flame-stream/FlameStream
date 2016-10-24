package experiments.interfaces.nikita.fake;

import experiments.interfaces.nikita.Condition;
import experiments.interfaces.nikita.RunningCondition;

/**
 * Created by marnikitta on 19.10.16.
 */
public class OddCondition implements Condition<Integer> {

    private final long maxEven;

    public OddCondition(final long maxEven) {
        this.maxEven = maxEven;
    }

    @Override
    public RunningCondition<Integer> instance() {
        return new RunningCondition<Integer>() {

            private int evenCount = 0;

            @Override
            public void update(final Integer value) {
                if (value % 2 == 0) {
                    ++evenCount;
                }
            }

            @Override
            public boolean isValid() {
                return evenCount < maxEven;
            }
        };
    }
}
