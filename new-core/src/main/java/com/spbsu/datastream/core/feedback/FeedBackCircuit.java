package com.spbsu.datastream.core.feedback;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class FeedBackCircuit implements AtomicGraph {

  // Ports
  private final List<InPort> ackPorts;
  private final List<OutPort> feedbackPorts;

  //Inner state
  private final Map<Long, Long> globalTsToXor = new HashMap<>();

  private final Map<Long, Integer> rootHashes = new HashMap<>();

  public FeedBackCircuit(final int ackers,
                          final int countSinks) {
    ackPorts = Stream.generate(() -> Ack.HASH_FUNCTION).map(InPort::new).limit(ackers).collect(Collectors.toList());
    feedbackPorts = Stream.generate(OutPort::new).limit(countSinks).collect(Collectors.toList());
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handle) {
    if (ackPorts.contains(inPort)) {
      final Ack ack = (Ack) item.payload();

      globalTsToXor.putIfAbsent(ack.globalTs(), 0L);
      rootHashes.putIfAbsent(ack.globalTs(), ack.rootHash());

      final long xor = globalTsToXor.computeIfPresent(
              ack.globalTs(),
              (globalTs, oldXor) -> oldXor ^ ack.ack());
      if (xor == 0) {
        closeDataItem(ack.globalTs(), handle, item.meta());
      }
    }
  }

  private void closeDataItem(final long globalTs, final AtomicHandle handle, final Meta lastAckMeta) {
    // TODO: 4/20/17

    rootHashes.remove(globalTs);
    globalTsToXor.remove(globalTs);
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.unmodifiableList(ackPorts);
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.unmodifiableList(feedbackPorts);
  }
}
