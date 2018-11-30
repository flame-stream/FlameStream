package com.spbsu.flamestream.example.bl.tfidf.ops.filtering;

import com.spbsu.flamestream.example.bl.tfidf.model.containers.WordDocContainer;
import com.spbsu.flamestream.example.bl.tfidf.model.entries.WordDocEntry;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class WordDocContainerSingleFilter implements Function<List<WordDocContainer>, Stream<List<WordDocContainer>>> {

    @Override
    public Stream<List<WordDocContainer>> apply(List<WordDocContainer> wordDocContainers) {
        if (wordDocContainers.size() > 2) {
            throw new IllegalStateException("Group size should be <= 2");
        }

        if (wordDocContainers.size() == 1 && !(wordDocContainers.get(0) instanceof WordDocEntry)) {
            throw new IllegalStateException("The only element in group should be WordEntry");
        }

        if (wordDocContainers.size() == 1) {
            return Stream.of(wordDocContainers);
        } else {
            return Stream.empty();
        }
    }
}