package experiments.interfaces.solar.bl;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class UserCounter implements UserContainer {
  @JsonProperty
  private String user;
  @JsonProperty
  private int count;

  public UserCounter() {}
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
