package com.spbsu.flamestream.example;

import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.ChaincallGraph;
import com.spbsu.flamestream.core.graph.Graph;
import com.spbsu.flamestream.core.graph.barrier.BarrierSuite;
import com.spbsu.flamestream.core.graph.ops.*;
import com.spbsu.flamestream.core.graph.source.impl.AbstractSource;
import com.spbsu.flamestream.core.graph.source.impl.SimpleSource;
import com.spbsu.flamestream.example.index.model.WikipediaPage;
import com.spbsu.flamestream.example.index.model.WordBase;
import com.spbsu.flamestream.example.index.model.WordPagePositions;
import com.spbsu.flamestream.example.index.ops.*;
import com.spbsu.flamestream.example.wordcount.model.WordContainer;
import com.spbsu.flamestream.example.wordcount.model.WordCounter;
import com.spbsu.flamestream.example.wordcount.model.WordEntry;
import com.spbsu.flamestream.example.wordcount.ops.CountWordEntries;
import com.spbsu.flamestream.example.wordcount.ops.WordContainerOrderingFilter;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 27.09.2017
 */
@SuppressWarnings({"Convert2Lambda", "NonSerializableFieldInSerializableClass"})
public enum FlameStreamExample {
  INVERTED_INDEX {
    private final HashFunction<WikipediaPage> wikiPageHash = new HashFunction<WikipediaPage>() {
      @Override
      public int hash(WikipediaPage value) {
        return HashFunction.UNIFORM_OBJECT_HASH.hash(value.id());
      }
    };

    private final HashFunction<WordBase> wordHash = new HashFunction<WordBase>() {
      @Override
      public int hash(WordBase value) {
        return HashFunction.UNIFORM_OBJECT_HASH.hash(value.word());
      }
    };

    private final BiPredicate<WordBase, WordBase> wordEqualz = new BiPredicate<WordBase, WordBase>() {
      @Override
      public boolean test(WordBase wordBase, WordBase wordBase2) {
        return wordBase.word().equals(wordBase2.word());
      }
    };

    private final HashFunction<List<WordBase>> groupHash = new HashFunction<List<WordBase>>() {
      @Override
      public int hash(List<WordBase> value) {
        return wordHash.hash(value.get(0));
      }
    };

    @Override
    public Graph graph(Function<ToIntFunction<?>, AtomicGraph> sinkBuilder) {
      final AbstractSource source = new SimpleSource();
      final Merge merge = new Merge(Arrays.asList(wordHash, wordHash));
      final Filter<WordBase> indexDiffFilter = new Filter<>(new WordIndexDiffFilter(), wordHash);
      final Grouping<WordBase> grouping = new Grouping<>(wordHash, wordEqualz, 2);
      final Filter<List<WordBase>> wrongOrderingFilter = new Filter<>(new WrongOrderingFilter(), groupHash);
      final FlatMap<List<WordBase>, WordBase> indexer = new FlatMap<>(new WordIndexToDiffOutput(), groupHash);
      final Filter<WordBase> indexFilter = new Filter<>(new WordIndexFilter(), wordHash);
      final Broadcast<WordBase> broadcast = new Broadcast<>(wordHash, 2);
      final BarrierSuite<WordBase> metaFilter = new BarrierSuite<>(sinkBuilder.apply(wordHash));

      final Graph chain = new ChaincallGraph(merge.fuse(grouping, merge.outPort(), grouping.inPort())
              .fuse(wrongOrderingFilter, grouping.outPort(), wrongOrderingFilter.inPort())
              .fuse(indexer, wrongOrderingFilter.outPort(), indexer.inPort())
              .fuse(broadcast, indexer.outPort(), broadcast.inPort())
              .fuse(indexFilter, broadcast.outPorts().get(1), indexFilter.inPort())
              .fuse(metaFilter, indexFilter.outPort(), metaFilter.inPort())
              .fuse(indexDiffFilter, broadcast.outPorts().get(0), indexDiffFilter.inPort())
              .wire(indexDiffFilter.outPort(), merge.inPorts().get(1))
              .flattened());

      final FlatMap<WikipediaPage, WordPagePositions> wikiPageToPositions = new FlatMap<>(
              new WikipediaPageToWordPositions(),
              wikiPageHash
      );
      return source.fuse(wikiPageToPositions, source.outPort(), wikiPageToPositions.inPort())
              .fuse(chain, wikiPageToPositions.outPort(), merge.inPorts().get(0));
    }
  },
  WORD_COUNT {
    private final HashFunction<WordContainer> wordHash = new HashFunction<WordContainer>() {
      @Override
      public int hash(WordContainer value) {
        return HashFunction.UNIFORM_OBJECT_HASH.hash(value.word());
      }
    };

    private final BiPredicate<WordContainer, WordContainer> equalz = new BiPredicate<WordContainer, WordContainer>() {
      @Override
      public boolean test(WordContainer o1, WordContainer o2) {
        return o1.word().equals(o2.word());
      }
    };

    private final HashFunction<List<WordContainer>> groupHash = new HashFunction<List<WordContainer>>() {
      @Override
      public int hash(List<WordContainer> value) {
        return wordHash.hash(value.get(0));
      }
    };

    @Override
    public Graph graph(Function<ToIntFunction<?>, AtomicGraph> sinkBuilder) {
      final AbstractSource source = new SimpleSource();
      final Merge merge = new Merge(Arrays.asList(wordHash, wordHash));
      final Grouping<WordContainer> grouping = new Grouping<>(wordHash, equalz, 2);
      final Filter<List<WordContainer>> filter = new Filter<>(new WordContainerOrderingFilter(), groupHash);
      final StatelessMap<List<WordContainer>, WordCounter> counter = new StatelessMap<>(
              new CountWordEntries(),
              groupHash
      );
      final Broadcast<WordCounter> broadcast = new Broadcast<>(wordHash, 2);
      final BarrierSuite<WordCounter> barrier = new BarrierSuite<>(sinkBuilder.apply(wordHash));

      final Graph logicChain = new ChaincallGraph(merge.fuse(grouping, merge.outPort(), grouping.inPort())
              .fuse(filter, grouping.outPort(), filter.inPort())
              .fuse(counter, filter.outPort(), counter.inPort())
              .fuse(broadcast, counter.outPort(), broadcast.inPort())
              .fuse(barrier, broadcast.outPorts().get(0), barrier.inPort())
              .wire(broadcast.outPorts().get(1), merge.inPorts().get(1))
              .flattened());

      final FlatMap<String, WordEntry> splitter = new FlatMap<>(new Function<String, Stream<WordEntry>>() {
        @Override
        public Stream<WordEntry> apply(String s) {
          return Arrays.stream(PATTERN.split(s)).map(WordEntry::new);
        }
      }, HashFunction.OBJECT_HASH);
      return source.fuse(splitter, source.outPort(), splitter.inPort())
              .fuse(logicChain, splitter.outPort(), merge.inPorts().get(0));
    }
  };

  private static final Pattern PATTERN = Pattern.compile("\\s");

  public abstract Graph graph(Function<ToIntFunction<?>, AtomicGraph> sinkBuilder);
}
