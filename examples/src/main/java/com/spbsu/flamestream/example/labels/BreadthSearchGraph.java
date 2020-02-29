package com.spbsu.flamestream.example.labels;

import scala.Tuple2;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class BreadthSearchGraph {
  static class VertexIdentifier {
  }

  static abstract class Input {
  }

  static class Request extends Input {
    static class Identifier {
    }

    final Identifier identifier;
    final VertexIdentifier vertexIdentifier;
    final int pathLength;

    Request(
            Identifier identifier,
            VertexIdentifier vertexIdentifier,
            int pathLength
    ) {
      this.identifier = identifier;
      this.vertexIdentifier = vertexIdentifier;
      this.pathLength = pathLength;
    }
  }

  static class VertexEdgesUpdate extends Input {
    final VertexIdentifier source;
    final List<VertexIdentifier> targets;

    VertexEdgesUpdate(VertexIdentifier source, List<VertexIdentifier> targets) {
      this.source = source;
      this.targets = targets;
    }
  }

  static class RequestKey {
    final Request.Identifier identifier;

    RequestKey(Request.Identifier identifier) {this.identifier = identifier;}

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof RequestKey) {
        return identifier.equals(((RequestKey) obj).identifier);
      }
      return super.equals(obj);
    }
  }

  public static class RequestOutput {
    final Request.Identifier requestIdentifier;
    final VertexIdentifier vertexIdentifier;

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

  private static class Agent {
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

  private static final Class<Either<RequestOutput, RequestKey>> EITHER_REQUEST_OUTPUT_OR_REQUEST_KEY_CLASS =
          (Class<Either<RequestOutput, RequestKey>>) (Class<?>) Either.class;
  private static final Class<Tuple2<Agent, Agent.ActionAfterVisit>> AGENT_WITH_ACTION_AFTER_VISIT_CLASS =
          (Class<Tuple2<Agent, Agent.ActionAfterVisit>>) (Class<?>) Tuple2.class;
  private static final Class<Either<Agent, VertexEdgesUpdate>> EITHER_AGENT_OR_VERTEX_EDGES_UPDATE_CLASS =
          (Class<Either<Agent, VertexEdgesUpdate>>) (Class<?>) Either.class;

  public static void main(String[] args) {
    mutableFlow(new HashMap<>());
  }

  public static Flow<Request, Either<RequestOutput, RequestKey>> immutableFlow(
          Map<VertexIdentifier, List<VertexIdentifier>> vertexEdges
  ) {
    final Operator.Input<Request> requestInput = new Operator.Input<>(Request.class);
    final Operator.LabelSpawn<Request, RequestKey> requestLabel = requestInput
            .spawnLabel(RequestKey.class, request -> new RequestKey(request.identifier));
    final Operator.Input<Agent> agentInput = new Operator.Input<>(Agent.class, Collections.singleton(requestLabel));
    agentInput.link(requestLabel.map(Agent.class, request ->
            new Agent(request.identifier, request.vertexIdentifier, request.pathLength)
    ));
    final Operator<Tuple2<Agent, Agent.ActionAfterVisit>> agentAndActionAfterVisit =
            agentAndActionAfterVisit(agentInput, requestLabel);
    agentInput.link(agentAndActionAfterVisit.flatMap(Agent.class, agent -> {
      if (agent._2 == Agent.ActionAfterVisit.Stop) {
        return Stream.empty();
      }
      return vertexEdges.getOrDefault(agent._1.vertexIdentifier, Collections.emptyList())
              .stream()
              .map(vertexIdentifier -> new Agent(
                      agent._1.requestIdentifier,
                      vertexIdentifier,
                      agent._1.remainingPathLength - 1
              ));
    }));
    final Operator.Input<Either<RequestOutput, RequestKey>> output =
            new Operator.Input<>(EITHER_REQUEST_OUTPUT_OR_REQUEST_KEY_CLASS);
    output.link(agentAndActionAfterVisit.flatMap(EITHER_REQUEST_OUTPUT_OR_REQUEST_KEY_CLASS, agent -> {
      if (agent._2 == Agent.ActionAfterVisit.VisitFirstTime) {
        return Stream.of(new Left<>(new RequestOutput(agent._1.requestIdentifier, agent._1.vertexIdentifier)));
      }
      return Stream.empty();
    }));
    output.link(
            agentAndActionAfterVisit
                    .labelMarkers(requestLabel)
                    .map(EITHER_REQUEST_OUTPUT_OR_REQUEST_KEY_CLASS, Right::apply)
    );
    return new Flow<>(requestInput, output);
  }

  public static Flow<Input, Either<RequestOutput, RequestKey>> mutableFlow(
          Map<VertexIdentifier, List<VertexIdentifier>> vertexEdges
  ) {
    final Operator.Input<Input> requestInput = new Operator.Input<>(Input.class);
    final Operator.LabelSpawn<Request, RequestKey> requestLabel = requestInput.flatMap(
            Request.class,
            input -> input instanceof Request ? Stream.of(((Request) input)) : Stream.empty()
    ).spawnLabel(RequestKey.class, request -> new RequestKey(request.identifier));
    final Operator.Input<Agent> agentInput = new Operator.Input<>(Agent.class, Collections.singleton(requestLabel));
    agentInput.link(requestLabel.map(
            Agent.class,
            request -> new Agent(request.identifier, request.vertexIdentifier, request.pathLength)
    ));
    final Operator.Input<Either<Agent, VertexEdgesUpdate>> vertexEdgesInput =
            new Operator.Input<>(EITHER_AGENT_OR_VERTEX_EDGES_UPDATE_CLASS);
    final Operator<Tuple2<Agent, Agent.ActionAfterVisit>> agentAndActionAfterVisit =
            agentAndActionAfterVisit(agentInput, requestLabel);
    agentInput.link(vertexEdgesInput
            .keyedBy(either -> either.isLeft() ? either.left().get().vertexIdentifier : either.right().get().source)
            .statefulFlatMap(Agent.class, (Either<Agent, VertexEdgesUpdate> either, List<VertexIdentifier> edges) -> {
              if (either.isLeft()) {
                final Agent agent = either.left().get();
                return new Tuple2<>(
                        edges,
                        (edges == null ? vertexEdges.getOrDefault(
                                agent.vertexIdentifier,
                                Collections.emptyList()
                        ) : edges).stream().map(vertexIdentifier -> new Agent(
                                agent.requestIdentifier,
                                vertexIdentifier,
                                agent.remainingPathLength - 1
                        ))
                );
              } else {
                return new Tuple2<>(either.right().get().targets, Stream.empty());
              }
            }, Collections.singleton(requestLabel)));
    final Operator.Input<Either<RequestOutput, RequestKey>> output =
            new Operator.Input<>(EITHER_REQUEST_OUTPUT_OR_REQUEST_KEY_CLASS);
    output.link(agentAndActionAfterVisit.flatMap(EITHER_REQUEST_OUTPUT_OR_REQUEST_KEY_CLASS, agent -> {
      if (agent._2 == Agent.ActionAfterVisit.VisitFirstTime) {
        return Stream.of(new Left<>(new RequestOutput(agent._1.requestIdentifier, agent._1.vertexIdentifier)));
      }
      return Stream.empty();
    }));
    output.link(
            agentAndActionAfterVisit
                    .labelMarkers(requestLabel)
                    .map(EITHER_REQUEST_OUTPUT_OR_REQUEST_KEY_CLASS, Right::apply)
    );
    return new Flow<>(requestInput, output);
  }

  private static Operator<Tuple2<Agent, Agent.ActionAfterVisit>> agentAndActionAfterVisit(
          Operator.Input<Agent> agentInput,
          Operator.LabelSpawn<Request, RequestKey> requestLabel
  ) {
    return agentInput
            .filter(agent -> agent.remainingPathLength > 0)
            .keyedBy(Collections.singleton(requestLabel), agent -> agent.vertexIdentifier)
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
}