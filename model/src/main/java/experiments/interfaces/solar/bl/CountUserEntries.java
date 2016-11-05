package experiments.interfaces.solar.bl;

import java.util.List;
import java.util.function.Function;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class CountUserEntries implements Function<List<UserContainer>, UserCounter> {
  @Override
  public UserCounter apply(List<UserContainer> containers) {
    final UserContainer first = containers.get(0);
    if (containers.size() < 2)
      return new UserCounter((UserQuery) first);
    else if (first instanceof UserQuery)
      return new UserCounter((UserCounter) containers.get(0), (UserQuery) first);
    else
      return null;
  }
}
