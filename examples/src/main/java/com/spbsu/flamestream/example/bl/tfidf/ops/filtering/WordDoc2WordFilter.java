package com.spbsu.flamestream.example.bl.tfidf.ops.filtering;

import com.spbsu.flamestream.example.bl.tfidf.model.containers.WordContainer;
import com.spbsu.flamestream.example.bl.tfidf.model.counters.WordDocCounter;
import com.spbsu.flamestream.example.bl.tfidf.model.entries.WordEntry;

import java.util.function.Function;
import java.util.stream.Stream;

public class WordDoc2WordFilter implements Function<WordDocCounter, Stream<WordContainer>> {
    @Override
    public Stream<WordContainer> apply(WordDocCounter wordDocCounter) {
        if (wordDocCounter.count() == 0) {
            return Stream.empty();
        } else {
            return Stream.of(new WordEntry(wordDocCounter.word()));
        }
    }
}