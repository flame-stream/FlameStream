package com.spbsu.datastream.core.feedback;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
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

public final class FeedBackCircuit extends AbstractAtomicGraph {

  // Ports
  private final List<InPort> ackPorts;
  private final List<OutPort> feedbackPorts;

  //Inner state
  private final Map<Long, Long> globalTsToXor = new HashMap<>();

  private final Map<Long, Integer> rootHashes = new HashMap<>();

  public FeedBackCircuit(final int ackers,
                          final int countSinks) {
    this.ackPorts = Stream.generate(() -> Ack.HASH_FUNCTION).map(InPort::new).limit(ackers).collect(Collectors.toList());
    this.feedbackPorts = Stream.generate(OutPort::new).limit(countSinks).collect(Collectors.toList());
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handle) {
    if (this.ackPorts.contains(inPort)) {
    }
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.unmodifiableList(this.ackPorts);
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.unmodifiableList(this.feedbackPorts);
  }
}
