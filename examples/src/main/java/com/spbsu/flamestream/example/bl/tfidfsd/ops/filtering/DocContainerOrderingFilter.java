package com.spbsu.flamestream.example.bl.tfidfsd.ops.filtering;

import com.spbsu.flamestream.example.bl.tfidfsd.model.IDFObject;
import com.spbsu.flamestream.example.bl.tfidfsd.model.TFObject;
import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidfsd.model.counters.WordCounter;
import com.spbsu.flamestream.example.bl.tfidfsd.model.entries.WordDocEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DocContainerOrderingFilter implements Function<List<DocContainer>, Stream<List<DocContainer>>> {

    @Override
    public Stream<List<DocContainer>> apply(List<DocContainer> docContainers) {
        if (docContainers.size() > 2) {
            throw new IllegalStateException("Group size should be <= 2");
        }

        if (docContainers.size() == 1 && !(docContainers.get(0) instanceof WordCounter)) {
            throw new IllegalStateException(String.format("The only element in group should be WordCounter: %s (%s)",
                    docContainers.get(0), docContainers.get(0).getClass()));
        }

        if (docContainers.size() == 1 || (docContainers.get(0) instanceof IDFObject
                && docContainers.get(1) instanceof WordCounter)) {
            if (docContainers.size() == 2 && !docContainers.get(0).document().equals(docContainers.get(1).document())) {
                return Stream.of(Collections.singletonList(docContainers.get(1)));
            }
            return Stream.of(docContainers);
        } else {
            return Stream.empty();
        }
    }
}
