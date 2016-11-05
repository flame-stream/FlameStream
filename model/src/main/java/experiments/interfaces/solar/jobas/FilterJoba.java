package experiments.interfaces.solar.jobas;

import experiments.interfaces.solar.DataItem;
import experiments.interfaces.solar.DataType;
import experiments.interfaces.solar.Joba;
import experiments.interfaces.solar.items.ObjectDataItem;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class FilterJoba extends Joba.Stub {
  private final Joba base;
  private final Function func;
  private final Class blInput;
  private final Class blOutput;

  public FilterJoba(Joba base, DataType generates, Function func, Class blInput, Class blOutput) {
    super(generates);
    this.base = base;
    this.func = func;
    this.blInput = blInput;
    this.blOutput = blOutput;
  }

  @Override
  public Stream<DataItem> materialize(Stream<DataItem> input) {
    return base.materialize(input).map(di -> {
      //noinspection unchecked
      final Object result = func.apply(di.as(blInput));
      if (result == null)
        return null;
      return (DataItem)new ObjectDataItem(result, blOutput, DataItem.Meta.advance(di.meta(), id()));
    }).filter(r -> r != null);
  }
}
