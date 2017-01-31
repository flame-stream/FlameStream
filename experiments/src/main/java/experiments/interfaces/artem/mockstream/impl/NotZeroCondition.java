package experiments.interfaces.artem.mockstream.impl;

import experiments.interfaces.artem.mockstream.Condition;
import experiments.interfaces.artem.mockstream.DataItem;

public class NotZeroCondition implements Condition {
    private static final Integer ZERO = 0;

    @Override
    public boolean update(DataItem item) {
        Integer data = item.data(Integer.class);
        return !ZERO.equals(data);
    }
}
