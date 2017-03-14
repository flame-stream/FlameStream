package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.ConstantSource;
import com.spbsu.datastream.core.HashableString;
import com.spbsu.datastream.core.graph.ops.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by marnikitta on 2/8/17.
 */
@SuppressWarnings("unchecked")
public class DeepCopyTest {
  @Test
  public void simpleLinear() {
    final Identity i1 = new Identity();
    final Identity i2 = new Identity();
    final Graph g = i1.fuse(i2, i1.outPort(), i2.inPort());
    final Graph copy = g.deepCopy();
  }

  @Test
  public void complex() {
    final Source<HashableString> source = new ConstantSource<>(new HashableString("1"));
    final Broadcast<HashableString> broadcast = new Broadcast(2);
    final StatelessFilter<HashableString, HashableString> f0 = new StatelessFilter<>(i -> i);
    final StatelessFilter<HashableString, HashableString> f1 = new StatelessFilter<>(i -> i);
    final Merge<HashableString> merge = new Merge<>(2);
    final ConsumerSink<HashableString> sink = new ConsumerSink<>(System.out::println);

    final Graph superGraph = source.fuse(broadcast, source.outPort(), broadcast.inPort()).fuse(f0, broadcast.outPorts().get(0), f0.inPort())
            .fuse(merge, f0.outPort(), merge.inPorts().get(0)).fuse(sink, merge.outPort(), sink.inPort())
            .compose(f1)
            .wire(broadcast.outPorts().get(1), f1.inPort()).wire(f1.outPort(), merge.inPorts().get(1));

    final Graph deepCopy = superGraph.deepCopy();

    final FlatGraph flatGraph = FlatGraph.flattened(superGraph);
    final FlatGraph flatCopy = FlatGraph.flattened(deepCopy);

    Assert.assertEquals(flatGraph.subGraphs().size(), flatCopy.subGraphs().size());

    final Source<HashableString> sourceCopy = (ConstantSource<HashableString>) flatCopy.subGraphs().stream()
            .filter(Source.class::isInstance).findAny().get();
    final Broadcast<HashableString> broadcastCopy = (Broadcast) flatCopy.subGraphs().stream()
            .filter(Broadcast.class::isInstance).findAny().get();
    final List<StatelessFilter> filtersCopy = flatCopy.subGraphs().stream()
            .filter(StatelessFilter.class::isInstance).map(StatelessFilter.class::cast)
            .collect(Collectors.toList());
    final StatelessFilter<HashableString, HashableString> f0Copy = filtersCopy.get(0).function().equals(f0.function()) ? filtersCopy.get(0) : filtersCopy.get(1);
    final StatelessFilter<HashableString, HashableString> f1Copy = filtersCopy.get(1).function().equals(f0.function()) ? filtersCopy.get(0) : filtersCopy.get(1);

    final Merge<HashableString> mergeCopy = (Merge) flatCopy.subGraphs().stream()
            .filter(Merge.class::isInstance).findAny().get();
    final ConsumerSink<HashableString> sinkCopy = (ConsumerSink) flatCopy.subGraphs().stream()
            .filter(ConsumerSink.class::isInstance).findAny().get();

    final Map<OutPort, InPort> expectedDownstreams = new HashMap<>();
    expectedDownstreams.put(sourceCopy.outPort(), broadcastCopy.inPort());
    expectedDownstreams.put(broadcastCopy.outPorts().get(0), f0Copy.inPort());
    expectedDownstreams.put(broadcastCopy.outPorts().get(1), f1Copy.inPort());
    expectedDownstreams.put(f0Copy.outPort(), mergeCopy.inPorts().get(0));
    expectedDownstreams.put(f1Copy.outPort(), mergeCopy.inPorts().get(1));
    expectedDownstreams.put(mergeCopy.outPort(), sinkCopy.inPort());
    Assert.assertEquals(flatCopy.downstreams(), expectedDownstreams);

    final Map<InPort, OutPort> expectedUpstreams = new HashMap<>();
    expectedUpstreams.put(sinkCopy.inPort(), mergeCopy.outPort());
    expectedUpstreams.put(mergeCopy.inPorts().get(0), f0Copy.outPort());
    expectedUpstreams.put(mergeCopy.inPorts().get(1), f1Copy.outPort());
    expectedUpstreams.put(f0Copy.inPort(), broadcastCopy.outPorts().get(0));
    expectedUpstreams.put(f1Copy.inPort(), broadcastCopy.outPorts().get(1));
    expectedUpstreams.put(broadcastCopy.inPort(), sourceCopy.outPort());
    Assert.assertEquals(flatCopy.upstreams(), expectedUpstreams);

    Assert.assertEquals(deepCopy.inPorts(), Collections.emptyList());
    Assert.assertEquals(deepCopy.outPorts(), Collections.emptyList());
  }
}
