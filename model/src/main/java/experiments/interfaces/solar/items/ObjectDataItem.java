package experiments.interfaces.solar.items;

import experiments.interfaces.solar.DataItem;
import experiments.interfaces.solar.DataTypeCollection;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class ObjectDataItem implements DataItem{
  private final Object item;
  private final Class clazz;
  private final Meta meta;

  public ObjectDataItem(Object item, Class clazz, Meta meta) {
    this.item = item;
    this.clazz = clazz;
    this.meta = meta;
  }


  @Override
  public Meta meta() {
    return meta;
  }

  @Override
  public CharSequence serializedData() {
    return DataTypeCollection.SERIALIZATION.write(item);
  }

  @Override
  public <T> T as(Class<T> type) {
    if (!type.isAssignableFrom(clazz))
      return null;
    //noinspection unchecked
    return (T)item;
  }
}
