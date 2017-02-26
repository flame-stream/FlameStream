package experiments.vova.impl.mock.Filters;

import experiments.vova.Filter;
import experiments.vova.Type;
import experiments.vova.impl.mock.IntType;

public class CountFilter implements Filter {
  public CountFilter() {
    count = 0;
    commitcount = 0;
  }

  @Override
  public boolean accepts(Type input) {
    return true;
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
    return new IntType();
  }

  @Override
  public Type inputType(Type output) {
    return null;
  }

  @Override
  public void commit() {
    commitcount = count;
  }

  @Override
  public void fail() {
    count = commitcount;
  }

  @Override
  public double weight() {
    return 1;
  }

  @Override
  public CharSequence apply(CharSequence charSequence) {
    count++;
    return Integer.toString(count);
  }

  private int count;
  private int commitcount;
}
