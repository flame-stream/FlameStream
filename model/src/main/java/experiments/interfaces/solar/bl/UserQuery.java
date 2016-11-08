package experiments.interfaces.solar.bl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class UserQuery implements UserContainer {
  @JsonProperty
  private String user;
  @JsonProperty
  private String query;

  public UserQuery(String user, String query) {
    this.user = user;
    this.query = query;
  }

  public  UserQuery() {}

  @Override
  public String user() {
    return user;
  }
}
