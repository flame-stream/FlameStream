package experiments.interfaces.vova;

import java.util.function.Function;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */
public interface Filter extends Function<CharSequence, CharSequence> {
  boolean accepts(Type input);
  boolean generates(Type output);
  Type outputType(Type input);
  Type inputType(Type output);
  void commit();
  void fail();
  double weight();
}
