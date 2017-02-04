package experiments.interfaces.nikita.akka.avoidingask.message;

/**
 * Created by marnikitta on 2/3/17.
 */
public class Get {
  private final int id;

  public Get(final int id) {
    this.id = id;
  }

  public int id() {
    return id;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Get{");
    sb.append("id=").append(id);
    sb.append('}');
    return sb.toString();
  }
}
