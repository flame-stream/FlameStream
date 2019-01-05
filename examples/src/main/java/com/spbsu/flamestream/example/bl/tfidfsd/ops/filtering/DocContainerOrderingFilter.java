package com.spbsu.flamestream.example.bl.tfidfsd.ops.filtering;

import com.spbsu.flamestream.example.bl.tfidfsd.model.TFObject;
import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidfsd.model.counters.WordCounter;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class DocContainerOrderingFilter implements Function<List<DocContainer>, Stream<List<DocContainer>>> {

    @Override
    public Stream<List<DocContainer>> apply(List<DocContainer> docContainers) {
        System.out.println("DCOF: " + docContainers);
        if (docContainers.size() > 2) {
            throw new IllegalStateException("Group size should be <= 2");
        }

        if (docContainers.size() == 1) {
            DocContainer single = docContainers.get(0);
            if (single instanceof WordCounter) {
                System.out.format("single word: %s%n", single);
                return Stream.of();
            }

            if (single instanceof TFObject) {
                return Stream.of();
            }

            throw new IllegalArgumentException("QQQQ");
        }

        if (docContainers.size() == 2) {
            //System.out.format("DCOF2: %s %s%n", docContainers.get(0).getClass(), docContainers.get(1).getClass());
            //System.out.format("DCOF22: %s %s%n", docContainers.get(0), docContainers.get(1));

            DocContainer first = docContainers.get(0);
            DocContainer second = docContainers.get(1);

            if (first instanceof TFObject && second instanceof TFObject) {
                return Stream.of();
            }
            if (first instanceof WordCounter && second instanceof WordCounter) {
                return Stream.of();
            }

            if (first instanceof WordCounter && second instanceof TFObject) {
                DocContainer t = first;
                first = second;
                second = t;
            }

            if (first instanceof TFObject && second instanceof WordCounter) {
                TFObject tfObject = (TFObject)first;
                WordCounter wc = (WordCounter) second;
                if (tfObject.isDefinedIdf(wc.word())) {
                    return Stream.of();
                } else {
                    return Stream.of(docContainers);
                }
            }

            return Stream.of(docContainers);
        }
        throw new IllegalArgumentException("QQQQ");
    }
}
