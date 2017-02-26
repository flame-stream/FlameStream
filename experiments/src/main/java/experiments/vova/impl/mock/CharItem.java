package experiments.vova.impl.mock;

import experiments.vova.DataItem;

public class CharItem implements DataItem {

  public CharItem(CharSequence data, Meta meta) {
    this.data = data;
    this.meta = meta;
  }

  @Override
  public Meta meta() {
    return meta;
  }

  @Override
  public CharSequence serializedData() {
    return data;
  }

  @Override
  public void setSerializedData(CharSequence data) {
    this.data = data;
  }

  Meta meta;
  CharSequence data;
}
