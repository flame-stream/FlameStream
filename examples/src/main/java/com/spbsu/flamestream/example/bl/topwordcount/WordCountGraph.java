package com.spbsu.flamestream.example.bl.topwordcount;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordContainer;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordCounter;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordEntry;
import com.spbsu.flamestream.example.bl.topwordcount.ops.CountWordEntries;
import com.spbsu.flamestream.example.bl.topwordcount.ops.CountWordsTop;
import com.spbsu.flamestream.example.bl.topwordcount.ops.WordContainerOrderingFilter;
import com.spbsu.flamestream.example.bl.topwordcount.ops.WordsTopOrderingFilter;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class WordCountGraph implements Supplier<Graph> {
  private final HashFunction wordHash = HashFunction.uniformHash(HashFunction.objectHash(WordContainer.class));

  @SuppressWarnings("Convert2Lambda")
  private final Equalz equalz = new Equalz() {
    @Override
    public boolean test(DataItem o1, DataItem o2) {
      return o1.payload(WordContainer.class).word().equals(o2.payload(WordContainer.class).word());
    }
  };

  @Override
  public Graph get() {
    final Source source = new Source();
    final FlameMap<String, WordEntry> splitter = new FlameMap<>(new Function<String, Stream<WordEntry>>() {
      private final Pattern pattern = Pattern.compile("\\s");

      @Override
      public Stream<WordEntry> apply(String s) {
        return Arrays.stream(pattern.split(s)).map(WordEntry::new);
      }
    }, String.class);
    final Grouping<WordContainer> wordGrouping = new Grouping<>(wordHash, equalz, 2, WordContainer.class);
    final FlameMap<List<WordContainer>, List<WordContainer>> wordFilter = new FlameMap<>(
            new WordContainerOrderingFilter(),
            List.class
    );
    final FlameMap<List<WordContainer>, WordCounter> wordCounter = new FlameMap<>(
            new CountWordEntries(),
            List.class
    );
    final Grouping<Object> topGrouping = new Grouping<>(HashFunction.constantHash(0), (o1, o2) -> true, 2, Object.class);
    final FlameMap<List<Object>, List<Object>> topFilter = new FlameMap<>(
            new WordsTopOrderingFilter(),
            List.class
    );
    final FlameMap<List<Object>, Object> topCounter = new FlameMap<>(
            new CountWordsTop(2),
            List.class
    );
    final Sink sink = new Sink();
    return new Graph.Builder()
            .link(source, splitter)
            .link(splitter, wordGrouping)
            .link(wordGrouping, wordFilter)
            .link(wordFilter, wordCounter)
            .link(wordCounter, wordGrouping)
            .link(wordCounter, topGrouping)
            .link(topGrouping, topFilter)
            .link(topFilter, topCounter)
            .link(topCounter, sink)
            .link(topCounter, topGrouping)
            .colocate(source, splitter)
            .colocate(wordGrouping, wordFilter, wordCounter, topGrouping, topFilter, topCounter, sink)
            .build(source, sink);
  }
}
