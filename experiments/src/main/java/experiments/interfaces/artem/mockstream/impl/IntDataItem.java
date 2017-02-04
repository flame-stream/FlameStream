package experiments.interfaces.artem.mockstream.impl;

import experiments.interfaces.artem.mockstream.DataItem;

public class IntDataItem implements DataItem {
  private Integer _data;
  private Meta _meta;

  public IntDataItem(Integer data) {
    _data = data;
    _meta = new SystemTimeMeta();
  }

  @Override
  public Meta meta() {
    return _meta;
  }

  @Override
  public CharSequence serializedData() {
    return null;
  }

  @Override
  public <T> T data(Class<T> type) {
    return type.cast(_data);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof IntDataItem))
      return false;

    return ((IntDataItem) obj).data(Integer.class).intValue() == _data.intValue();
  }
}