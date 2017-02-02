package experiments.interfaces.nikita.akka.cluster.worker;

import com.spbsu.datastream.core.DataItem;

/**
 * Created by marnikitta on 2/2/17.
 */
public interface StatefulMapper {
  Object updatedState(Object previousState, DataItem newItem);
}
