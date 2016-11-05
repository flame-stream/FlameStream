package experiments.interfaces.solar;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class SystemTime {
  public static final SystemTime ZERO = new SystemTime() {
    @Override
    public boolean greater(SystemTime time) {
      return false;
    }
  };
  public static final SystemTime INFINITY = new SystemTime() {
    @Override
    public boolean greater(SystemTime time) {
      return true;
    }
  };

  private final long globalTime;
  private final int localTime;

  private static volatile int currentLocalTime = 0;
  private SystemTime() {
    globalTime = -1;
    localTime = currentLocalTime++;
  }

  public SystemTime(long globalTime) {
    this.globalTime = globalTime;
    localTime = currentLocalTime++;
  }

  public boolean greater(SystemTime time) {
    return time.globalTime == globalTime ? localTime > time.localTime : globalTime > time.globalTime;
  }

  public int tick() {
    return (int)(globalTime / 10000);
  }

  public long global() {
    return globalTime;
  }
}
