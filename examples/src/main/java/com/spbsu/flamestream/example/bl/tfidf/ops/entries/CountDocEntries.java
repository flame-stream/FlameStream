package com.spbsu.flamestream.example.bl.tfidf.ops.entries;

import com.spbsu.flamestream.example.bl.tfidf.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidf.model.counters.DocCounter;
import com.spbsu.flamestream.example.bl.tfidf.model.entries.DocEntry;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;


public class CountDocEntries implements Function<List<DocContainer>, Stream<DocCounter>> {
    @Override
    public Stream<DocCounter> apply(List<DocContainer> docContainers) {
        if (docContainers.size() == 1) {
            final DocEntry docEntry = (DocEntry) docContainers.get(0);
            return Stream.of(new DocCounter(docEntry, 1));
        } else {
            final DocCounter counter = (DocCounter) docContainers.get(0);
            return Stream.of(new DocCounter(counter.docEntry(), counter.count() + 1));
        }
    }
}
