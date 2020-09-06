package com.spbsu.flamestream.example.graph_search;

import com.google.common.hash.Hashing;
import com.spbsu.flamestream.core.graph.HashGroup;
import com.spbsu.flamestream.core.graph.SerializableConsumer;
import com.spbsu.flamestream.core.graph.SerializableFunction;
import com.spbsu.flamestream.example.labels.Flow;
import com.spbsu.flamestream.example.labels.Operator;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BreadthSearchGraph {
  public static final class VertexIdentifier {
    public final int id;

    public VertexIdentifier(int id) {this.id = id;}

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof VertexIdentifier) {
        return id == ((VertexIdentifier) obj).id;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return id;
    }
  }

  public static abstract class Input {
  }

  public static final class Request extends Input {
    public static final class Identifier {
      public final int id;
      private final int hashCode;

      public Identifier(int id) {
        this.id = id;
        hashCode = Hashing.murmur3_32().hashInt(id).hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) {
          return true;
        }
        if (obj instanceof Identifier) {
          return id == ((Identifier) obj).id;
        }
        return false;
      }

      @Override
      public int hashCode() {
        return hashCode;
      }
    }

    public final Identifier identifier;
    public final VertexIdentifier vertexIdentifier;
    public final int pathLength;

    public Request(
            Identifier identifier,
            VertexIdentifier vertexIdentifier,
            int pathLength
    ) {
      this.identifier = identifier;
      this.vertexIdentifier = vertexIdentifier;
      this.pathLength = pathLength;
    }
  }

  static final class VertexEdgesUpdate extends Input {
    final VertexIdentifier source;
    final List<VertexIdentifier> targets;

    VertexEdgesUpdate(VertexIdentifier source, List<VertexIdentifier> targets) {
      this.source = source;
      this.targets = targets;
    }
  }

  public static final class RequestOutput {
    public final Request.Identifier requestIdentifier;
    public final VertexIdentifier vertexIdentifier;

    public RequestOutput(Request.Identifier requestIdentifier, VertexIdentifier vertexIdentifier) {
      this.requestIdentifier = requestIdentifier;
      this.vertexIdentifier = vertexIdentifier;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof RequestOutput) {
        final RequestOutput other = (RequestOutput) obj;
        return requestIdentifier.equals(other.requestIdentifier) && vertexIdentifier.equals(other.vertexIdentifier);
      }
      return super.equals(obj);
    }
  }

  private static final class Agent {
    final Request.Identifier requestIdentifier;
    final VertexIdentifier vertexIdentifier;
    final int remainingPathLength;

    enum ActionAfterVisit {
      Stop,
      VisitFirstTime,
      Revisit
    }

    Agent(
            Request.Identifier requestIdentifier,
            VertexIdentifier vertexIdentifier,
            int remainingPathLength
    ) {
      this.requestIdentifier = requestIdentifier;
      this.vertexIdentifier = vertexIdentifier;
      this.remainingPathLength = remainingPathLength;
    }
  }

  public static final Class<Either<RequestOutput, Request.Identifier>> OUTPUT_CLASS =
          (Class<Either<RequestOutput, Request.Identifier>>) (Class<?>) Either.class;
  private static final Class<Tuple2<Agent, Agent.ActionAfterVisit>> AGENT_WITH_ACTION_AFTER_VISIT_CLASS =
          (Class<Tuple2<Agent, Agent.ActionAfterVisit>>) (Class<?>) Tuple2.class;
  private static final Class<Either<Agent, VertexEdgesUpdate>> EITHER_AGENT_OR_VERTEX_EDGES_UPDATE_CLASS =
          (Class<Either<Agent, VertexEdgesUpdate>>) (Class<?>) Either.class;

  public interface HashedVertexEdges {
    Stream<VertexIdentifier> apply(VertexIdentifier vertexIdentifier);

    int hash(VertexIdentifier vertexIdentifier);
  }

  private static final class VertexEdges
          implements SerializableConsumer<HashGroup>, Function<VertexIdentifier, Stream<VertexIdentifier>> {
    final SerializableFunction<HashGroup, HashedVertexEdges> initializer;
    transient HashedVertexEdges initialized;

    private VertexEdges(SerializableFunction<HashGroup, HashedVertexEdges> initializer) {
      this.initializer = initializer;
    }

    @Override
    public void accept(HashGroup hashGroup) {
      initialized = initializer.apply(hashGroup);
    }

    @Override
    public Stream<VertexIdentifier> apply(VertexIdentifier vertexIdentifier) {
      return initialized.apply(vertexIdentifier);
    }

    int hash(VertexIdentifier vertexIdentifier) {
      return initialized.hash(vertexIdentifier);
    }
  }

  public static Flow<Request, Either<RequestOutput, Request.Identifier>> immutableFlow(
          SerializableFunction<HashGroup, HashedVertexEdges> vertexEdgesSupplier
  ) {
    final VertexEdges vertexEdges = new VertexEdges(vertexEdgesSupplier);
    final Operator.Input<Request> requestInput = new Operator.Input<>(Request.class);
    final Operator.LabelSpawn<Request, Request.Identifier> requestLabel = requestInput
            .spawnLabel(Request.Identifier.class, request -> request.identifier);
    final Operator.Input<Agent> agentInput = new Operator.Input<>(Agent.class, Collections.singleton(requestLabel));
    agentInput.link(requestLabel.map(Agent.class, request ->
            new Agent(request.identifier, request.vertexIdentifier, request.pathLength)
    ));
    final Operator<Tuple2<Agent, Agent.ActionAfterVisit>> agentAndActionAfterVisit =
            agentAndActionAfterVisit(agentInput, requestLabel, vertexEdges);
    agentInput.link(agentAndActionAfterVisit.flatMap(Agent.class, agent -> {
      if (agent._2 == Agent.ActionAfterVisit.Stop) {
        return Stream.empty();
      }
      final int remainingPathLength = agent._1.remainingPathLength - 1;
      if (remainingPathLength < 0) {
        return Stream.empty();
      }
      return vertexEdges.apply(agent._1.vertexIdentifier).map(vertexIdentifier -> new Agent(
              agent._1.requestIdentifier,
              vertexIdentifier,
              remainingPathLength
      ));
    }));
    return new Flow<>(requestInput, output(agentAndActionAfterVisit, requestLabel), vertexEdges);
  }

  public static Flow<Input, Either<RequestOutput, Request.Identifier>> mutableFlow(
          SerializableFunction<HashGroup, HashedVertexEdges> vertexEdgesSupplier
  ) {
    final VertexEdges vertexEdges = new VertexEdges(vertexEdgesSupplier);
    final Operator.Input<Input> requestInput = new Operator.Input<>(Input.class);
    final Operator.LabelSpawn<Request, Request.Identifier> requestLabel = requestInput.flatMap(
            Request.class,
            input -> input instanceof Request ? Stream.of((Request) input) : Stream.empty()
    ).spawnLabel(Request.Identifier.class, request -> request.identifier);
    final Operator.Input<Agent> agentInput = new Operator.Input<>(Agent.class, Collections.singleton(requestLabel));
    agentInput.link(requestLabel.map(Agent.class, request ->
            new Agent(request.identifier, request.vertexIdentifier, request.pathLength)
    ));
    final Operator.Input<Either<Agent, VertexEdgesUpdate>> vertexEdgesInput =
            new Operator.Input<>(EITHER_AGENT_OR_VERTEX_EDGES_UPDATE_CLASS);
    final Operator<Tuple2<Agent, Agent.ActionAfterVisit>> agentAndActionAfterVisit =
            agentAndActionAfterVisit(agentInput, requestLabel, vertexEdges);
    agentInput.link(vertexEdgesInput.newKeyedBuilder(
            either1 -> either1.isLeft() ? either1.left().get().vertexIdentifier : either1.right().get().source
    ).build()
            .statefulFlatMap(Agent.class, (Either<Agent, VertexEdgesUpdate> either, List<VertexIdentifier> edges) -> {
              if (either.isLeft()) {
                final Agent agent = either.left().get();
                if (edges == null) {
                  edges = vertexEdges.apply(agent.vertexIdentifier).collect(Collectors.toList());
                }
                final int remainingPathLength = agent.remainingPathLength - 1;
                if (remainingPathLength < 0) {
                  return new Tuple2<>(edges, Stream.empty());
                }
                return new Tuple2<>(edges, edges.stream().map(vertexIdentifier -> new Agent(
                        agent.requestIdentifier,
                        vertexIdentifier,
                        remainingPathLength
                )));
              } else {
                return new Tuple2<>(either.right().get().targets, Stream.empty());
              }
            }, Collections.singleton(requestLabel)));
    return new Flow<>(requestInput, output(agentAndActionAfterVisit, requestLabel), vertexEdges);
  }

  private static Operator<Tuple2<Agent, Agent.ActionAfterVisit>> agentAndActionAfterVisit(
          Operator.Input<Agent> agentInput,
          Operator.LabelSpawn<Request, Request.Identifier> requestLabel,
          VertexEdges vertexEdges
  ) {
    return agentInput.newKeyedBuilder(agent -> agent.vertexIdentifier, agent -> -agent.remainingPathLength)
            .keyLabels(Collections.singleton(requestLabel)).hash(vertexEdges::hash).build()
            .statefulMap(AGENT_WITH_ACTION_AFTER_VISIT_CLASS, (Agent agent, Integer remainingPathLength) -> {
              final Agent.ActionAfterVisit actionAfterVisit;
              if (remainingPathLength == null) {
                actionAfterVisit = Agent.ActionAfterVisit.VisitFirstTime;
                remainingPathLength = agent.remainingPathLength;
              } else if (remainingPathLength < agent.remainingPathLength) {
                actionAfterVisit = Agent.ActionAfterVisit.Revisit;
                remainingPathLength = agent.remainingPathLength;
              } else {
                actionAfterVisit = Agent.ActionAfterVisit.Stop;
              }
              return new Tuple2<>(remainingPathLength, new Tuple2<>(agent, actionAfterVisit));
            }, Collections.singleton(requestLabel));
  }

  private static Operator<Either<RequestOutput, Request.Identifier>> output(
          Operator<Tuple2<Agent, Agent.ActionAfterVisit>> agentAndActionAfterVisit,
          Operator.LabelSpawn<Request, Request.Identifier> requestLabel
  ) {
    return agentAndActionAfterVisit.flatMap(RequestOutput.class, agent -> {
      if (agent._2 == Agent.ActionAfterVisit.VisitFirstTime) {
        return Stream.of(new RequestOutput(agent._1.requestIdentifier, agent._1.vertexIdentifier));
      }
      return Stream.empty();
    }).labelMarkers(requestLabel, null);
  }

  private static <T> List<T> scalaStreamToJava(scala.collection.immutable.Vector<T> scalaStream) {
    return new ArrayList<>(JavaConverters.seqAsJavaListConverter(scalaStream).asJava());
  }
}
