package experiments.interfaces.vova;

import java.util.Set;

public interface Type {
    String name();
    Set<Class<? extends Condition>> conditions();
}
