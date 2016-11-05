package experiments.interfaces.solar.items;

import experiments.interfaces.solar.DataItem;

import java.util.ArrayList;
import java.util.List;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class ListDataItem extends ArrayList<DataItem> implements DataItem {
  public ListDataItem(List<DataItem> copy) {

  }

  public ListDataItem() {

  }

  @Override
  public Meta meta() {
    return null;
  }

  @Override
  public CharSequence serializedData() {
    return null;
  }

  @Override
  public <T> T as(Class<T> type) {
    return null;
  }
}
