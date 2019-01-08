package com.spbsu.flamestream.example.bl.tfidfsd.ops.filtering;

import com.spbsu.flamestream.example.bl.tfidfsd.model.IDFObject;
import com.spbsu.flamestream.example.bl.tfidfsd.model.TFObject;
import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidfsd.model.counters.WordCounter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DocContainerOrderingFilter implements Function<List<DocContainer>, Stream<List<DocContainer>>> {

    @Override
    public Stream<List<DocContainer>> apply(List<DocContainer> docContainers) {
        System.out.println("DCOF: " + docContainers);
        if (docContainers.size() > 2) {
            throw new IllegalStateException("Group size should be <= 2");
        }

        if (docContainers.size() == 1) {
            System.out.println("DCOF-1: " + docContainers);
            DocContainer single = docContainers.get(0);
            if (single instanceof IDFObject) {
                System.out.format("single IDFObject: %s%n", single);
                return Stream.of(Arrays.asList(single));
            }

            if (single instanceof TFObject) {
                System.out.format("single TFObject: %s%n", single);
                return Stream.of(Arrays.asList(single));
            }

            throw new IllegalArgumentException("QQQQ");
        }

        if (docContainers.size() == 2) {
            //System.out.format("DCOF2: %s %s%n", docContainers.get(0).getClass(), docContainers.get(1).getClass());
            //System.out.format("DCOF22: %s %s%n", docContainers.get(0), docContainers.get(1));
            System.out.println("DCOF-2: " + docContainers);

            DocContainer first = docContainers.get(0);
            DocContainer second = docContainers.get(1);

            if (first instanceof TFObject && second instanceof TFObject) {
                return Stream.of();
            }
            if (first instanceof IDFObject && second instanceof IDFObject) {
                return Stream.of(Arrays.asList(first, second));
            }

            if (first instanceof IDFObject && second instanceof TFObject) {
                DocContainer t = first;
                first = second;
                second = t;
            }

            if (first instanceof TFObject && second instanceof IDFObject) {
                final TFObject tfObject = (TFObject)first;
                IDFObject idfObject = (IDFObject) second;


                List<String> relevant = idfObject.keys().stream().filter(w -> !tfObject.isDefinedIdf(w)).collect(Collectors.toList());

                //return Stream.of(Arrays.asList(tfObject, new IDFObject(idfObject.document(), relevant)));
                return Stream.of();
            }

            return Stream.of(/*docContainers*/);
        }
        throw new IllegalArgumentException("QQQQ");
    }
}
