package com.spbsu.flamestream.example.index;

import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.example.index.model.WordBase;
import com.spbsu.flamestream.example.index.validators.RankingValidator;
import com.spbsu.flamestream.example.index.validators.SmallDumpRankingValidator;
import com.spbsu.flamestream.example.index.validators.SmallDumpValidator;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.util.AwaitConsumer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * User: Artem
 * Date: 19.12.2017
 */
public class InvertedIndexTest extends FlameStreamSuite {
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
    final LocalRuntime runtime = new LocalRuntime(4, 1);
    final FlameRuntime.Flame flame = runtime.run(new InvertedIndexGraph().get());
    {
      final AwaitConsumer<WordBase> awaitConsumer = new AwaitConsumer<>(validator.expectedOutputSize());
      flame.attachRear("Rear", new AkkaRearType<>(runtime.system(), WordBase.class))
              .forEach(r -> r.addListener(awaitConsumer));

      final Consumer<Object> sink = randomConsumer(
              flame.attachFront("Sink", new AkkaFrontType<>(runtime.system(), backPressure))
                      .collect(Collectors.toList())
      );
      validator.input().forEach(sink);

      awaitConsumer.await(5, TimeUnit.MINUTES);
      validator.assertCorrect(awaitConsumer.result());
    }
  }
}
