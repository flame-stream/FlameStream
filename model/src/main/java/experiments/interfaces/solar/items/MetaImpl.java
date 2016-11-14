package experiments.interfaces.solar.items;

import experiments.interfaces.solar.DataItem;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class MetaImpl implements DataItem.Meta<MetaImpl> {
  public static final MetaImpl ZERO = new MetaImpl() {
    @Override
    public int compareTo(final MetaImpl o) {
      return -1;
    }
  };

  public static final MetaImpl INFINITY = new MetaImpl() {
    @Override
    public int compareTo(final MetaImpl o) {
      return 1;
    }
  };

  private final long globalTime;
  private final int localTime;

  private static volatile int currentLocalTime = 0;

  private MetaImpl() {
    globalTime = -1;
    localTime = currentLocalTime++;
  }

  public MetaImpl(long globalTime) {
    this.globalTime = globalTime;
    localTime = currentLocalTime++;
  }

  public int tick() {
    return (int) (globalTime / 10000000);
  }

  @Override
  public MetaImpl advance() {
    return new MetaImpl(globalTime);
  }

  @Override
  public int compareTo(final MetaImpl o) {
    return globalTime == o.globalTime ? Integer.compare(localTime, o.localTime) : Long.compare(globalTime, o.globalTime);
  }
}
