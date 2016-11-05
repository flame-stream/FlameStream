package experiments.interfaces.solar.bl;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class UserCounter implements UserContainer {
  private final String user;
  private final int count;

  public UserCounter(UserQuery userQuery) {
    user = userQuery.user();
    count = 1;
  }

  @SuppressWarnings("UnusedParameters")
  public UserCounter(UserCounter userCounter, UserQuery first) {
    user = userCounter.user();
    count = userCounter.count + 1;
  }

  @Override
  public String user() {
    return user;
  }
}
