package com.spbsu.flamestream.example.bl.tfidf.ops.entries;

import com.spbsu.flamestream.example.bl.tfidf.model.containers.WordDocContainer;
import com.spbsu.flamestream.example.bl.tfidf.model.counters.WordDocCounter;
import com.spbsu.flamestream.example.bl.tfidf.model.entries.WordDocEntry;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class CountWordDocEntries implements Function<List<WordDocContainer>, Stream<WordDocCounter>> {
    @Override
    public Stream<WordDocCounter> apply(List<WordDocContainer> wordDocContainers) {
        if (wordDocContainers.size() == 1) {
            final WordDocEntry wordDocEntry = (WordDocEntry) wordDocContainers.get(0);
            return Stream.of(new WordDocCounter(wordDocEntry, 1));
        } else {
            final WordDocCounter counter = (WordDocCounter) wordDocContainers.get(0);
            return Stream.of(new WordDocCounter(counter.wordDocEntry(), counter.count() + 1));
        }
    }
}