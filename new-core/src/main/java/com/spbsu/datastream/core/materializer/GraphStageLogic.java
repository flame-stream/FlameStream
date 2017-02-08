package com.spbsu.datastream.core.materializer;

import akka.actor.ActorRef;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.materializer.ShardConcierge;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by marnikitta on 2/8/17.
 */
public abstract class GraphStageLogic {
  private final Set<InPort> inPorts;
  private final Set<OutPort> outPorts;

  private final Map<OutPort, ActorRef> portsMapping = new HashMap<>();

  private ShardConcierge context;

  protected GraphStageLogic(final Set<InPort> inPorts, final Set<OutPort> outPorts) {
    this.inPorts = inPorts;
    this.outPorts = outPorts;
  }

  abstract <T> void onPush(InPort inPort, T element);

  final protected <S> void commitState(final S state) {

  }

  final protected <R> void push(final OutPort out, final R result) {
    assertValidPort(out);
  }

  private void assertValidPort(final OutPort port) {
    if (!this.outPorts.contains(port)) {
      throw new IllegalArgumentException("This stage hasn't got such port");
    }
  }

  final void concierge(final ShardConcierge context) {
    this.context = context;
  }
}
