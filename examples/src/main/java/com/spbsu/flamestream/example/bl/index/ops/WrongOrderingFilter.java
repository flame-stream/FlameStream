package com.spbsu.flamestream.example.bl.index.ops;

import com.spbsu.flamestream.example.bl.index.model.WordBase;
import com.spbsu.flamestream.example.bl.index.model.WordIndex;
import com.spbsu.flamestream.example.bl.index.model.WordPagePositions;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WrongOrderingFilter implements Function<List<WordBase>, Stream<List<WordBase>>> {
  private transient final Tracing.Tracer tracer = Tracing.TRACING.forEvent("wrong-ordering-filter-receive");

  @Override
  public Stream<List<WordBase>> apply(List<WordBase> wordBases) {
    if (wordBases.size() > 2) {
      throw new IllegalStateException("Group size should be <= 2");
    }
    if (wordBases.size() == 1 && !(wordBases.get(0) instanceof WordPagePositions)) {
      throw new IllegalStateException("The only element in group should be WordPagePosition");
    }

    if (wordBases.size() == 1 || (wordBases.get(0) instanceof WordIndex
            && wordBases.get(1) instanceof WordPagePositions)) {
      final WordPagePositions positions = (WordPagePositions) wordBases.get(wordBases.size() == 1 ? 0 : 1);
      tracer.log(Objects.hash(positions.word(), IndexItemInLong.pageId(positions.positions()[0])));
      return Stream.of(wordBases);
    } else {
      return Stream.empty();
    }
  }
}
