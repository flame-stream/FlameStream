package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class LocalRuntimeTest {
  @Test
  public void initTest() throws InterruptedException {
    final FlameRuntime runtime = new LocalRuntime(10);
    final Source source = new Source();
    final Sink sink = new Sink();
    final Graph graph = new Graph.Builder().link(source, sink).build(source, sink);
    runtime.run(graph);
    TimeUnit.SECONDS.sleep(10);
  }
}