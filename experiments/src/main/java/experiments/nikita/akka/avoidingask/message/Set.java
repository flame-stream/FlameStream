package experiments.nikita.akka.avoidingask.message;

/**
 * Created by marnikitta on 2/3/17.
 */
public class Set {
  private final int id;

  private final String value;

  public Set(final int id, final String value) {
    this.id = id;
    this.value = value;
  }

  public int id() {
    return id;
  }

  public String value() {
    return value;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Set{");
    sb.append("id=").append(id);
    sb.append(", value='").append(value).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
