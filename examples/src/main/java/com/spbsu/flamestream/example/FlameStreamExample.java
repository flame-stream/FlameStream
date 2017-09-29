package com.spbsu.flamestream.example;

import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.ChaincallGraph;
import com.spbsu.flamestream.core.graph.Graph;
import com.spbsu.flamestream.core.graph.barrier.BarrierSuite;
import com.spbsu.flamestream.core.graph.ops.*;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
import com.spbsu.flamestream.example.inverted_index.model.WordBase;
import com.spbsu.flamestream.example.inverted_index.model.WordPagePositions;
import com.spbsu.flamestream.example.inverted_index.ops.*;
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
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 27.09.2017
 */
@SuppressWarnings({"Convert2Lambda", "Anonymous2MethodRef"})
public enum FlameStreamExample {
  INVERTED_INDEX {
    private final HashFunction<WikipediaPage> WIKI_PAGE_HASH = new HashFunction<WikipediaPage>() {
      @Override
      public int hash(WikipediaPage value) {
        return value.id();
      }
    };

    private final HashFunction<WordBase> WORD_HASH = new HashFunction<WordBase>() {
      @Override
      public int hash(WordBase value) {
        return value.word().hashCode();
      }
    };

    private final BiPredicate<WordBase, WordBase> WORD_EQUALZ = new BiPredicate<WordBase, WordBase>() {
      @Override
      public boolean test(WordBase wordBase, WordBase wordBase2) {
        return wordBase.word().equals(wordBase.word());
      }
    };

    private final HashFunction<List<WordBase>> GROUP_HASH = new HashFunction<List<WordBase>>() {
      @Override
      public int hash(List<WordBase> value) {
        return WORD_HASH.hash(value.get(0));
      }
    };

    @Override
    public Graph graph(Function<ToIntFunction<?>, AtomicGraph> sinkBuilder) {
      final Merge merge = new Merge(Arrays.asList(WORD_HASH, WORD_HASH));
      final Filter<WordBase> indexDiffFilter = new Filter<>(new WordIndexDiffFilter(), WORD_HASH);
      final Grouping<WordBase> grouping = new Grouping<>(WORD_HASH, WORD_EQUALZ, 2);
      final Filter<List<WordBase>> wrongOrderingFilter = new Filter<>(new WrongOrderingFilter(), GROUP_HASH);
      final FlatMap<List<WordBase>, WordBase> indexer = new FlatMap<>(new WordIndexToDiffOutput(), GROUP_HASH);
      final Filter<WordBase> indexFilter = new Filter<>(new WordIndexFilter(), WORD_HASH);
      final Broadcast<WordBase> broadcast = new Broadcast<>(WORD_HASH, 2);
      final BarrierSuite<WordBase> metaFilter = new BarrierSuite<>(sinkBuilder.apply(WORD_HASH));

      final AtomicGraph chain = new ChaincallGraph(
              merge.fuse(grouping, merge.outPort(), grouping.inPort())
                      .fuse(wrongOrderingFilter, grouping.outPort(), wrongOrderingFilter.inPort())
                      .fuse(indexer, wrongOrderingFilter.outPort(), indexer.inPort())
                      .fuse(broadcast, indexer.outPort(), broadcast.inPort())
                      .fuse(indexFilter, broadcast.outPorts().get(1), indexFilter.inPort())
                      .fuse(metaFilter, indexFilter.outPort(), metaFilter.inPort())
                      .fuse(indexDiffFilter, broadcast.outPorts().get(0), indexDiffFilter.inPort())
                      .wire(indexDiffFilter.outPort(), merge.inPorts().get(1))
                      .flattened()
      );

      final FlatMap<WikipediaPage, WordPagePositions> wikiPageToPositions = new FlatMap<>(new WikipediaPageToWordPositions(), WIKI_PAGE_HASH);
      return wikiPageToPositions.fuse(chain, wikiPageToPositions.outPort(), merge.inPorts().get(0));
    }
  },
  WORD_COUNT {
    private final HashFunction<WordContainer> WORD_HASH = new HashFunction<WordContainer>() {
      @Override
      public int hash(WordContainer value) {
        return HashFunction.jenkinsHash(value.word().hashCode());
      }
    };

    private final BiPredicate<WordContainer, WordContainer> EQUALZ = new BiPredicate<WordContainer, WordContainer>() {
      @Override
      public boolean test(WordContainer o1, WordContainer o2) {
        return o1.word().equals(o2.word());
      }
    };

    private final HashFunction<List<WordContainer>> GROUP_HASH = new HashFunction<List<WordContainer>>() {
      @Override
      public int hash(List<WordContainer> value) {
        return WORD_HASH.hash(value.get(0));
      }
    };

    @Override
    public Graph graph(Function<ToIntFunction<?>, AtomicGraph> sinkBuilder) {
      final Merge merge = new Merge(Arrays.asList(WORD_HASH, WORD_HASH));
      final Grouping<WordContainer> grouping = new Grouping<>(WORD_HASH, EQUALZ, 2);
      final Filter<List<WordContainer>> filter = new Filter<>(new WordContainerOrderingFilter(), GROUP_HASH);
      final StatelessMap<List<WordContainer>, WordCounter> counter = new StatelessMap<>(new CountWordEntries(), GROUP_HASH);
      final Broadcast<WordCounter> broadcast = new Broadcast<>(WORD_HASH, 2);
      final BarrierSuite<WordCounter> barrier = new BarrierSuite<>(sinkBuilder.apply(WORD_HASH));

      final ChaincallGraph logicChain = new ChaincallGraph(
              merge.fuse(grouping, merge.outPort(), grouping.inPort())
                      .fuse(filter, grouping.outPort(), filter.inPort())
                      .fuse(counter, filter.outPort(), counter.inPort())
                      .fuse(broadcast, counter.outPort(), broadcast.inPort())
                      .fuse(barrier, broadcast.outPorts().get(0), barrier.inPort())
                      .wire(broadcast.outPorts().get(1), merge.inPorts().get(1))
                      .flattened()
      );

      final FlatMap<String, WordEntry> splitter = new FlatMap<>(new Function<String, Stream<WordEntry>>() {
        @Override
        public Stream<WordEntry> apply(String s) {
          return Arrays.stream(s.split("\\s")).map(WordEntry::new);
        }
      }, HashFunction.OBJECT_HASH);
      return splitter.fuse(logicChain, splitter.outPort(), merge.inPorts().get(0));
    }
  };

  public abstract Graph graph(Function<ToIntFunction<?>, AtomicGraph> sinkBuilder);
}
