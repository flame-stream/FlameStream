package experiments.interfaces.solar.bl;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class UserQuery implements UserContainer {
  private final String user;
  private final String query;

  public UserQuery(String user, String query) {
    this.user = user;
    this.query = query;
  }


  @Override
  public String user() {
    return user;
  }
}
