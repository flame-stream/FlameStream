package com.spbsu.flamestream.example.benchmark;

import akka.actor.Address;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.socket.SocketFrontType;
import com.spbsu.flamestream.runtime.edge.socket.SocketRearType;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class FlameGraphDeployer implements GraphDeployer {
  private final FlameRuntime runtime;
  private final Graph graph;
  private final SocketFrontType frontType;
  private final SocketRearType rearType;

  public FlameGraphDeployer(FlameRuntime runtime,
                            Graph graph,
                            SocketFrontType frontType,
                            SocketRearType rearType) {
    this.runtime = runtime;
    this.graph = graph;
    this.frontType = frontType;
    this.rearType = rearType;
  }

  @Override
  public Map<String, Handle> deploy() {
    final FlameRuntime.Flame flame = runtime.run(graph);
    return Stream.concat(
            flame.attachFront("FlameSocketGraphDeployerFront", frontType),
            flame.attachRear("FlameSocketGraphDeployerRear", rearType)
    ).collect(Collectors.toSet()).stream().collect(Collectors.toMap(
            edgeContext -> edgeContext.edgeId().nodeId(),
            edgeContext -> {
              final Address address = edgeContext.nodePath().address();
              final InetSocketAddress inetSocketAddress = new InetSocketAddress(
                      address.host().get(),
                      ((scala.Int) address.port().get()).toInt()
              );
              return new Handle(inetSocketAddress, inetSocketAddress);
            }
    ));
  }

  @Override
  public void close() {
    try {
      runtime.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
