package experiments.interfaces.solar;

/**
 * Experts League
 * Created by solar on 27.10.16.
 */
@SuppressWarnings("WeakerAccess")
public class Main {
  public static void main(String[] args) {
    try {
      final DataTypeCollection types = DataTypeCollection.instance();
      final Joba joba = types.<Integer>convert(types.type("UsersLog"), types.type("Frequences"));
      Input.instance().stream(types.type("UsersLog")).flatMap(
          joba::materialize // recovery mechanism on exception here
      ).forEach(Output.instance().printer());
    }
    catch(TypeUnreachableException ignored) {
   }
  }

}
