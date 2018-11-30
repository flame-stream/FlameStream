package com.spbsu.flamestream.example.bl.tfidf.ops.filtering;

import com.spbsu.flamestream.example.bl.tfidf.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidf.model.counters.DocCounter;
import com.spbsu.flamestream.example.bl.tfidf.model.entries.DocEntry;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class DocContainerOrderingFilter implements Function<List<DocContainer>, Stream<List<DocContainer>>> {

    @Override
    public Stream<List<DocContainer>> apply(List<DocContainer> docContainers) {
        if (docContainers.size() > 2) {
            throw new IllegalStateException("Group size should be <= 2");
        }

        if (docContainers.size() == 1 && !(docContainers.get(0) instanceof DocEntry)) {
            throw new IllegalStateException("The only element in group should be DocEntry");
        }

        if (docContainers.size() == 1 || (docContainers.get(0) instanceof DocCounter
                && docContainers.get(1) instanceof DocEntry)) {
            return Stream.of(docContainers);
        } else {
            return Stream.empty();
        }
    }
}