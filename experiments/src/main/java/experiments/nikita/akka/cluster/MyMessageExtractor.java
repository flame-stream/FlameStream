package experiments.nikita.akka.cluster;

import akka.cluster.sharding.ShardRegion;

/**
 * Created by marnikitta on 2/2/17.
 */
public class MyMessageExtractor implements ShardRegion.MessageExtractor {
  @Override
  public String entityId(final Object message) {
    if (message instanceof Envelope) {
      final Envelope envelope = (Envelope) message;
      return String.valueOf(envelope.id());
    } else {
      return null;
    }
  }

  @Override
  public Object entityMessage(final Object message) {
    if (message instanceof Envelope) {
      final Envelope envelope = (Envelope) message;
      return envelope.payload();
    } else {
      return null;
    }
  }

  @Override
  public String shardId(final Object message) {
    int numberOfShards = 4;
    if (message instanceof Envelope) {
      long id = ((Envelope) message).id();
      return String.valueOf(id % numberOfShards);
    } else {
      return null;
    }
  }
}
