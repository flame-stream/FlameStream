package com.spbsu.flamestream.example.bl.index;

import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import com.spbsu.flamestream.example.bl.index.model.WordBase;
import com.spbsu.flamestream.example.bl.index.model.WordPagePositions;
import com.spbsu.flamestream.example.bl.index.ops.WikipediaPageToWordPositions;
import com.spbsu.flamestream.example.bl.index.ops.WordIndexDiffFilter;
import com.spbsu.flamestream.example.bl.index.ops.WordIndexFilter;
import com.spbsu.flamestream.example.bl.index.ops.WordIndexToDiffOutput;
import com.spbsu.flamestream.example.bl.index.ops.WrongOrderingFilter;

import java.util.List;
import java.util.function.Supplier;

public class InvertedIndexGraph implements Supplier<Graph> {
  private final HashFunction wordHash = HashFunction.uniformHash(
          dataItem -> dataItem.payload(WordBase.class).word().hashCode()
  );

  private final Equalz wordEqualz = (dataItem, dataItem2) ->
          dataItem.payload(WordBase.class).word().equals(dataItem2.payload(WordBase.class).word());

  @Override
  public Graph get() {
    final Source source = new Source();
    final FlameMap<WikipediaPage, WordPagePositions> wikiPageToPositions = new FlameMap.Builder<>(
            new WikipediaPageToWordPositions(),
            WikipediaPage.class
    ).hashFunction(HashFunction.objectHash(WikipediaPage.class)).build();
    final FlameMap<WordBase, WordBase> indexDiffFilter = new FlameMap.Builder<>(
            new WordIndexDiffFilter(),
            WordBase.class
    ).build();
    final Grouping<WordBase> grouping = new Grouping<>(wordHash, wordEqualz, 2, WordBase.class);
    final FlameMap<List<WordBase>, List<WordBase>> wrongOrderingFilter = new FlameMap.Builder<>(
            new WrongOrderingFilter(),
            List.class
    ).build();
    final FlameMap<List<WordBase>, WordBase> indexer = new FlameMap.Builder<>(
            new WordIndexToDiffOutput(),
            List.class
    ).build();
    final FlameMap<WordBase, WordBase> indexFilter = new FlameMap.Builder<>(
            new WordIndexFilter(),
            WordBase.class
    ).build();
    final Sink sink = new Sink();

    return new Graph.Builder()
            .link(source, wikiPageToPositions)
            //.colocate(source, wikiPageToPositions)
            .link(wikiPageToPositions, grouping)
            .link(grouping, wrongOrderingFilter)
            .link(wrongOrderingFilter, indexer)
            .link(indexer, indexFilter)
            .link(indexer, indexDiffFilter)
            .colocate(grouping, wrongOrderingFilter, indexDiffFilter, indexer, indexFilter, indexDiffFilter, sink)
            .link(indexDiffFilter, grouping)
            .link(indexFilter, sink)
            .build(source, sink);
  }
}
