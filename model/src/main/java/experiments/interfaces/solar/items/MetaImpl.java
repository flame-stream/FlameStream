package experiments.interfaces.solar.items;

import experiments.interfaces.solar.DataItem;
import experiments.interfaces.solar.SystemTime;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class MetaImpl implements DataItem.Meta {
  private final SystemTime time;

  public MetaImpl(DataItem.Meta meta, int id) {
    time = new SystemTime(meta.time().global());
  }

  public MetaImpl(SystemTime time) {
    this.time = time;
  }

  @Override
  public SystemTime time() {
    return time;
  }

  @Override
  public int tick() {
    return time.tick();
  }
}
