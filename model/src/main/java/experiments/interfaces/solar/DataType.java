package experiments.interfaces.solar;

import java.util.Collections;
import java.util.Set;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public interface DataType {
  DataType UNTYPED = new DataType() {
    @Override
    public String name() {
      return "untyped";
    }

    @Override
    public Set<Class<? extends Condition>> conditions() {
      return Collections.emptySet();
    }
  };

  String name();

  Set<Class<? extends Condition>> conditions();

  class Stub implements DataType {
    private String name;

    @Override
    public String name() {
      return name;
    }

    @Override
    public Set<Class<? extends Condition>> conditions() {
      return Collections.emptySet();
    }

    public Stub(String name) {
      this.name = name;
    }
  }
}
