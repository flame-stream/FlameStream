package experiments.nikita.akka.streaming;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.*;

import java.util.Arrays;

/**
 * Created by marnikitta on 2/6/17.
 */
@SuppressWarnings("unchecked")
public class StreamingMain {
  public static void main(final String... args) {
    final ActorSystem system = ActorSystem.create();

    final ActorMaterializer materializer = ActorMaterializer.create(system);


    final Graph<FlowShape<Integer, Integer>, NotUsed> partial =
            GraphDSL.create(builder -> {
              final UniformFanOutShape<Integer, Integer> B = builder.add(Broadcast.create(2));
              final UniformFanInShape<Integer, Integer> C = builder.add(Merge.create(2));
              final UniformFanOutShape<Integer, Integer> E = builder.add(Balance.create(2));
              final UniformFanInShape<Integer, Integer> F = builder.add(Merge.create(2));
              builder.from(F.out()).toInlet(C.in(0));
              builder.from(B).viaFanIn(C).toFanIn(F);
              builder.from(B).via(builder.add(Flow.of(Integer.class).map(i -> i + 1))).viaFanOut(E).toFanIn(F);
              return new FlowShape<Integer, Integer>(B.in(), E.out(1));
            });
    System.out.println(partial.module().toString());

    final Source<Integer, NotUsed> source = Source.fromGraph(
            GraphDSL.create(builder -> {
              final UniformFanInShape<Integer, Integer> merge = builder.add(Merge.create(2));
              builder.from(builder.add(Source.single(0))).toFanIn(merge);
              builder.from(builder.add(Source.from(Arrays.asList(2, 3, 4)))).toFanIn(merge);
              return new SourceShape<>(merge.out());
            })
    );


    final Sink<Integer, NotUsed> sink = Flow.of(Integer.class)
            .map(i -> {
              System.out.println(i);
              return i * 2;
            })
            .drop(10)
            .named("nestedFlow")
            .to(Sink.head());

    final RunnableGraph<NotUsed> closed = source.via(partial).to(sink);

    closed.run(materializer);
  }
}
