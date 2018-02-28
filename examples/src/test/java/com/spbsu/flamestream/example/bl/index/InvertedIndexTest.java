package com.spbsu.flamestream.example.bl.index;

import com.spbsu.flamestream.example.bl.index.model.WordBase;
import com.spbsu.flamestream.example.bl.index.validators.RankingValidator;
import com.spbsu.flamestream.example.bl.index.validators.SmallDumpRankingValidator;
import com.spbsu.flamestream.example.bl.index.validators.SmallDumpValidator;
import com.spbsu.flamestream.runtime.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * User: Artem
 * Date: 19.12.2017
 */
public class InvertedIndexTest extends FlameAkkaSuite {
  @DataProvider
  public static Object[][] provider() {
    return new Object[][] {
            {new SmallDumpValidator(), false},
            {new SmallDumpRankingValidator(), false},
            {new RankingValidator(), false},
            {new RankingValidator(), true}
    };
  }

  @Test(dataProvider = "provider")
  public void test(InvertedIndexValidator validator, boolean backPressure) throws InterruptedException {
    try (final LocalRuntime runtime = new LocalRuntime(DEFAULT_PARALLELISM, 10)) {
      final FlameRuntime.Flame flame = runtime.run(new InvertedIndexGraph().get());
      {
        final String invocationConfig = validator.getClass().getSimpleName() + "-bp-" + backPressure;
        final AwaitResultConsumer<WordBase> awaitConsumer = new AwaitResultConsumer<>(validator.expectedOutputSize());
        flame.attachRear("Rear-" + invocationConfig, new AkkaRearType<>(runtime.system(), WordBase.class))
                .forEach(r -> r.addListener(awaitConsumer));

        final List<AkkaFrontType.Handle<Object>> consumers =
                flame.attachFront("Front-" + invocationConfig, new AkkaFrontType<>(runtime.system(), backPressure))
                        .collect(Collectors.toList());
        for (int i = 1; i < consumers.size(); i++) {
          consumers.get(i).eos();
        }

        final AkkaFrontType.Handle<Object> sink = consumers.get(0);
        validator.input().forEach(sink);
        sink.eos();

        awaitConsumer.await(5, TimeUnit.MINUTES);
        validator.assertCorrect(awaitConsumer.result());
      }
    }
  }
}
