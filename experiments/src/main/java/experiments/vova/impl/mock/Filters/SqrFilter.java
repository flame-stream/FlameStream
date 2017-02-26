package experiments.vova.impl.mock.Filters;

import experiments.vova.Filter;
import experiments.vova.Type;
import experiments.vova.impl.mock.IntType;

public class SqrFilter implements Filter {

  @Override
  public boolean accepts(Type input) {
    if (input.name().equals("int")) {
      return true;
    }
    return false;
  }

  @Override
  public boolean generates(Type output) {
    if (output.name().equals("int")) {
      return true;
    }
    return false;
  }

  @Override
  public Type outputType(Type input) {
    if (input.name().equals("int")) {
      return new IntType();
    }
    return null;
  }

  @Override
  public Type inputType(Type output) {
    if (output.name().equals("int")) {
      return new IntType();
    }
    return null;
  }

  @Override
  public void commit() {
  }

  @Override
  public void fail() {
  }

  @Override
  public double weight() {
    return 1;
  }

  @Override
  public CharSequence apply(CharSequence charSequence) {
    int x = Integer.parseInt(charSequence.toString());
    return Integer.toString(x * x);
  }
}
