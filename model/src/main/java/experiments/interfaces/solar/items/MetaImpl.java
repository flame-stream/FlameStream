package experiments.interfaces.solar.items;

import experiments.interfaces.solar.DataItem;
import experiments.interfaces.solar.SystemTime;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class MetaImpl implements DataItem.Meta<MetaImpl> {
  private final SystemTime time;

  public MetaImpl(SystemTime time) {
    this.time = time;
  }

  @Override
  public int tick() {
    return time.tick();
  }

  @Override
  public MetaImpl advance() {
    return new MetaImpl(this.time);
  }

  @Override
  public int compareTo(final MetaImpl o) {
    return this.time.compareTo(o.time);
  }
}
