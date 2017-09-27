package com.spbsu.flamestream.example;

import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.ChaincallGraph;
import com.spbsu.flamestream.core.graph.Graph;
import com.spbsu.flamestream.core.graph.barrier.BarrierSink;
import com.spbsu.flamestream.core.graph.barrier.PreBarrierMetaFilter;
import com.spbsu.flamestream.core.graph.ops.*;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
import com.spbsu.flamestream.example.inverted_index.model.WordContainer;
import com.spbsu.flamestream.example.inverted_index.model.WordPagePositions;
import com.spbsu.flamestream.example.inverted_index.ops.*;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * User: Artem
 * Date: 27.09.2017
 */
@SuppressWarnings({"Convert2Lambda", "Anonymous2MethodRef"})
public enum FlameStreamExamples {
  INVERTED_INDEX {
    private final HashFunction<WikipediaPage> WIKI_PAGE_HASH = new HashFunction<WikipediaPage>() {
      @Override
      public int hash(WikipediaPage value) {
        return value.id();
      }
    };

    private final HashFunction<WordContainer> WORD_HASH = new HashFunction<WordContainer>() {
      @Override
      public int hash(WordContainer value) {
        return value.word().hashCode();
      }
    };

    private final BiPredicate<WordContainer, WordContainer> WORD_EQUALZ = new BiPredicate<WordContainer, WordContainer>() {
      @Override
      public boolean test(WordContainer wordContainer, WordContainer wordContainer2) {
        return wordContainer.word().equals(wordContainer.word());
      }
    };

    private final HashFunction<List<WordContainer>> GROUP_HASH = new HashFunction<List<WordContainer>>() {
      @Override
      public int hash(List<WordContainer> value) {
        return WORD_HASH.hash(value.get(0));
      }
    };

    public Graph graph(BarrierSink barrierSink) {
      final Merge merge = new Merge(Arrays.asList(WORD_HASH, WORD_HASH));
      final Filter<WordContainer> indexDiffFilter = new Filter<>(new WordIndexDiffFilter(), WORD_HASH);
      final Grouping<WordContainer> grouping = new Grouping<>(WORD_HASH, WORD_EQUALZ, 2);
      final Filter<List<WordContainer>> wrongOrderingFilter = new Filter<>(new WrongOrderingFilter(), GROUP_HASH);
      final FlatMap<List<WordContainer>, WordContainer> indexer = new FlatMap<>(new WordIndexToDiffOutput(), GROUP_HASH);
      final Filter<WordContainer> indexFilter = new Filter<>(new WordIndexFilter(), WORD_HASH);
      final Broadcast<WordContainer> broadcast = new Broadcast<>(WORD_HASH, 2);
      final PreBarrierMetaFilter<WordContainer> metaFilter = new PreBarrierMetaFilter<>(WORD_HASH);

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
      return wikiPageToPositions
              .fuse(chain, wikiPageToPositions.outPort(), merge.inPorts().get(0))
              .fuse(barrierSink, metaFilter.outPort(), barrierSink.inPort());
    }
  };

  public abstract Graph graph(BarrierSink barrierSink);
}
