package experiments.interfaces.artem.mockstream;

import java.util.function.Function;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */
public interface Filter extends Function<CharSequence, CharSequence> {
  boolean accepts(DataStream.Type input);

  boolean generates(DataStream.Type output);

  DataStream.Type outputType(DataStream.Type input);

  DataStream.Type inputType(DataStream.Type output);

  double weight();
}
