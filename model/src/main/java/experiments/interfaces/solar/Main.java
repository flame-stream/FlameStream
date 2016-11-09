package experiments.interfaces.solar;

/**
 * Experts League
 * Created by solar on 27.10.16.
 */
@SuppressWarnings("WeakerAccess")
public class Main {
  public static void main(String[] args) {
    final DataTypeCollection types = DataTypeCollection.instance();
    Input.instance().stream(types.type("UsersLog")).flatMap((input) -> {
      final Joba joba;
      try {
        joba = types.<Integer>convert(types.type("UsersLog"), types.type("Frequences"));
        return joba.materialize(input).onClose(() -> Output.instance().commit()); // recovery mechanism on exception her
      }
      catch (TypeUnreachableException tue) {
        throw new RuntimeException(tue);
      }
    }).forEach(Output.instance().printer());
  }

}
