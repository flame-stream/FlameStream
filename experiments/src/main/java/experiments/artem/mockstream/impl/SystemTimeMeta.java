package experiments.artem.mockstream.impl;

import experiments.artem.mockstream.DataItem;

class SystemTimeMeta implements DataItem.Meta {
  private long _time;

  SystemTimeMeta() {
    _time = System.currentTimeMillis();
  }

  @Override
  public long time() {
    return _time;
  }
}
