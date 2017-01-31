package experiments.interfaces.vova.impl.mock;

import experiments.interfaces.vova.Condition;
import experiments.interfaces.vova.Type;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by nuka3 on 11/6/16.
 */
public class IntType implements Type {

    @Override
    public String name() {
        return "int";
    }

    @Override
    public Set<Class<? extends Condition>> conditions() {
        Set<Class<? extends Condition>> s = new HashSet<>();
        s.add(IntCondition.class);
        return s;
    }
}
