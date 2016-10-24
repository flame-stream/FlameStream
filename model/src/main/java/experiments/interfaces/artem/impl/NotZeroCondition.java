package experiments.interfaces.artem.impl;

import experiments.interfaces.artem.Condition;
import experiments.interfaces.artem.DataItem;

public class NotZeroCondition implements Condition {
    private static final Integer ZERO = 0;

    @Override
    public boolean update(DataItem item) {
        Integer data = item.data(Integer.class);
        return !ZERO.equals(data);
    }
}
