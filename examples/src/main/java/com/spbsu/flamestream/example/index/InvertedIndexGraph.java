package com.spbsu.flamestream.example.index;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.example.index.model.WikipediaPage;
import com.spbsu.flamestream.example.index.model.WordBase;
import com.spbsu.flamestream.example.index.model.WordPagePositions;
import com.spbsu.flamestream.example.index.ops.WikipediaPageToWordPositions;
import com.spbsu.flamestream.example.index.ops.WordIndexDiffFilter;
import com.spbsu.flamestream.example.index.ops.WordIndexFilter;
import com.spbsu.flamestream.example.index.ops.WordIndexToDiffOutput;
import com.spbsu.flamestream.example.index.ops.WrongOrderingFilter;

import java.util.List;
import java.util.function.Supplier;

public class InvertedIndexGraph implements Supplier<Graph> {
  private final HashFunction wordHash = HashFunction.uniformHash(HashFunction.objectHash(WordBase.class));

  private final Equalz wordEqualz = new Equalz() {
    @Override
    public boolean test(DataItem dataItem, DataItem dataItem2) {
      return dataItem.payload(WordBase.class).word().equals(dataItem2.payload(WordBase.class).word());
    }
  };

  @Override
  public Graph get() {
    final Source source = new Source();
    final FlameMap<WikipediaPage, WordPagePositions> wikiPageToPositions = new FlameMap<>(
            new WikipediaPageToWordPositions(),
            WikipediaPage.class
    );
    final FlameMap<WordBase, WordBase> indexDiffFilter = new FlameMap<>(new WordIndexDiffFilter(), WordBase.class);
    final Grouping<WordBase> grouping = new Grouping<>(wordHash, wordEqualz, 2, WordBase.class);
    final FlameMap<List<WordBase>, List<WordBase>> wrongOrderingFilter = new FlameMap<>(
            new WrongOrderingFilter(),
            List.class
    );
    final FlameMap<List<WordBase>, WordBase> indexer = new FlameMap<>(new WordIndexToDiffOutput(), List.class);
    final FlameMap<WordBase, WordBase> indexFilter = new FlameMap<>(new WordIndexFilter(), WordBase.class);
    final Sink sink = new Sink();

    return new Graph.Builder()
            .link(source, wikiPageToPositions)
            .link(wikiPageToPositions, grouping)
            .link(grouping, wrongOrderingFilter)
            .link(wrongOrderingFilter, indexer)
            .link(indexer, indexFilter)
            .link(indexer, indexDiffFilter)
            .link(indexDiffFilter, grouping)
            .link(indexFilter, sink)
            .build(source, sink);
  }
}
