package experiments.vova.impl.mock;

import experiments.vova.Condition;
import experiments.vova.DataItem;

/**
 * Created by nuka3 on 11/6/16.
 */
public class IntCondition implements Condition {

  @Override
  public boolean update(DataItem item) {
    CharSequence s = item.serializedData();
    for (int i = 0; i < s.length(); i++) {
      if ((s.charAt(i) < '0' || s.charAt(i) > '9') && (s.charAt(i) != '-')) {
        return false;
      }
    }
    return true;
  }
}
