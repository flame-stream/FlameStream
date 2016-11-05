package experiments.interfaces.solar;

import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.seq.CharSeqTools;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
class Input {
  private static Input instance = new Input();

  public static synchronized Input instance() {
    return instance;
  }

  private Stream<Stream<DataItem>> ticks(InputStream is) {
    return CharSeqTools.lines(new InputStreamReader(is, StreamTools.UTF), false)
        .map(DataItem::fromCharSeq)
        .collect(Collectors.groupingBy(di -> di.meta().tick()))
        .values().stream()
        .map(collection -> {
          final Stream<DataItem> stream = collection.stream();
          stream.onClose(() -> Output.instance().commit());
          return stream;
        });
  }

  @SuppressWarnings("UnusedParameters")
  public Stream<Stream<DataItem>> stream(DataType type) {
    return ticks(System.in);
  }
}
