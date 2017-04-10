package com.spbsu.datastream.core.feedback;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.Meta;
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

  private FeedBackCircuit(final List<HashFunction<?>> ackHashes,
                          final int countSinks) {
    ackPorts = ackHashes.stream().map(InPort::new).collect(Collectors.toList());
    feedbackPorts = Stream.generate(OutPort::new).limit(countSinks).collect(Collectors.toList());
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handle) {
    if (ackPorts.contains(inPort)) {
      final Ack ack = (Ack) item.payload();

      final long xor = globalTsToXor.compute(ack.globalTs(),
              (globalTs, oldXor) -> oldXor == null ? ack.ack() : oldXor ^ ack.ack());
      if (xor == 0) {
        closeDataItem(ack.globalTs(), handle, item.meta());
      }
    }
  }

  private void closeDataItem(final long globalTs, final AtomicHandle handle, final Meta lastAckMeta) {
    globalTsToXor.remove(globalTs);

    for (int i = 0; i < feedbackPorts.size(); ++i) {
      handle.push(
              feedbackPorts.get(i),
              new NoAckDataItem<>(handle.copyAndAppendLocal(lastAckMeta, true), new DICompeted(globalTs))
      );
    }
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
