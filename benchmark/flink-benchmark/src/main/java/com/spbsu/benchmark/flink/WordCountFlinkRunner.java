package com.spbsu.benchmark.flink;

/**
 * User: Artem
 * Date: 08.09.2017
 */
public class WordCountFlinkRunner {
  /*private final Logger LOG = LoggerFactory.getLogger(WordCountFlinkRunner.class);

  private static Iterator<String> iterator;
  private static Consumer<Object> consumer;

  @Override
  public void run(Cluster cluster) throws InterruptedException {
    final LatencyMeasurer<WordCounter> latencyMeasurer = new LatencyMeasurer<>(1000 * 10, 1000 * 10);

    final TObjectIntMap<String> expected = new TObjectIntHashMap<>();
    final Stream<String> source = WordCountRunner.input()
            .peek(
                    text -> {
                      final Pattern pattern = Pattern.compile("\\s");
                      Arrays.stream(pattern.split(text))
                              .collect(toMap(Function.identity(), o -> 1, Integer::sum))
                              .forEach((k, v) -> {
                                expected.adjustOrPutValue(k, v, v);
                                latencyMeasurer.start(new WordCounter(k, expected.get(k)));
                              });
                    }
            );

    test(source, o -> latencyMeasurer.finish((WordCounter) o), 0);

    final LongSummaryStatistics stat = Arrays.stream(latencyMeasurer.latencies())
            .map(TimeUnit.NANOSECONDS::toMillis)
            .summaryStatistics();
    LOG.info("Result: {}", stat);
  }

  static void test(Stream<String> source, Consumer<Object> output, int bufferTimeout) {
    try {
      iterator = source.iterator();
      consumer = output;

      final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
      env.setBufferTimeout(bufferTimeout);

      //noinspection deprecation
      env.addSource(new TextSource())
              .flatMap(new Splitter())
              .keyBy(0)
              //fold is deprecated but there is no alternative in the current version
              .fold(new WordCounter(null, 0), new WordCounterFold())
              .addSink(wordCounter -> consumer.accept(wordCounter));

      env.execute();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodError ignore) {
      //akka now does not have shutdown method
    }
  }

  private static class TextSource implements SourceFunction<String> {
    private boolean running = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
      //noinspection Duplicates
      while (running) {
        if (iterator.hasNext()) {
          ctx.collect(iterator.next());
          ctx.emitWatermark(new Watermark(System.nanoTime()));
        } else {
          running = false;
          ctx.close();
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  private static class Splitter implements FlatMapFunction<String, Tuple1<String>> {
    @Override
    public void flatMap(String value, Collector<Tuple1<String>> out) throws Exception {
      Arrays.stream(value.split("\\s")).forEach(s -> out.collect(new Tuple1<>(s)));
    }
  }

  @SuppressWarnings("deprecation")
  private static class WordCounterFold implements FoldFunction<Tuple1<String>, WordCounter> {
    @Override
    public WordCounter fold(WordCounter accumulator, Tuple1<String> value) throws Exception {
      return new WordCounter(value.f0, accumulator.count() + 1);
    }
  }*/
}
