package experiments.nikita.akka.cluster;

/**
 * Created by marnikitta on 2/2/17.
 */
public class Envelope {
  private final long id;

  private final Object payload;

  public Envelope(final long id, final Object payload) {
    this.id = id;
    this.payload = payload;
  }

  public long id() {
    return id;
  }

  public Object payload() {
    return payload;
  }
}
