package com.spbsu.flamestream.example.bl.wordcount;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.example.bl.wordcount.model.WordContainer;
import com.spbsu.flamestream.example.bl.wordcount.model.WordCounter;
import com.spbsu.flamestream.example.bl.wordcount.model.WordEntry;
import com.spbsu.flamestream.example.bl.wordcount.ops.CountWordEntries;
import com.spbsu.flamestream.example.bl.wordcount.ops.WordContainerOrderingFilter;

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
    final Grouping<WordContainer> grouping = new Grouping<>(wordHash, equalz, 2, WordContainer.class);
    final FlameMap<List<WordContainer>, List<WordContainer>> filter = new FlameMap<>(
            new WordContainerOrderingFilter(),
            List.class
    );
    final FlameMap<List<WordContainer>, WordCounter> counter = new FlameMap<>(
            new CountWordEntries(),
            List.class
    );
    final Sink sink = new Sink();
    return new Graph.Builder()
            .link(source, splitter)
            .link(splitter, grouping)
            .link(grouping, filter)
            .link(filter, counter)
            .link(counter, sink)
            .link(counter, grouping)
            .build(source, sink);
  }
}
