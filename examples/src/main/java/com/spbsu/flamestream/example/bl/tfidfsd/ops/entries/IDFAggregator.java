package com.spbsu.flamestream.example.bl.tfidfsd.ops.entries;

//import com.spbsu.flamestream.example.bl.tfidfsd.model.IDFObject;
//import com.spbsu.flamestream.example.bl.tfidfsd.model.IDFObject;
import com.spbsu.flamestream.example.bl.tfidfsd.model.TFObject;
import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidfsd.model.counters.DocCounter;
import com.spbsu.flamestream.example.bl.tfidfsd.model.counters.WordCounter;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class IDFAggregator implements Function<List<DocContainer>, Stream<DocContainer>> {
    @Override
    public Stream<DocContainer> apply(List<DocContainer> docContainers) {
        //System.out.println("DC: " + docContainers);

        if (docContainers.size() == 1) {
            final DocContainer single = docContainers.get(0);
            //if (single instanceof IDFObject) return Stream.of(single);

            /*
            if (single instanceof WordCounter) {
                WordCounter wc = (WordCounter) single;
                return Stream.of(new IDFObject(single.document(), wc.word(), wc.count()));
            }
            */
            //if (single instanceof IDFObject) return Stream.of(single);
            if (single instanceof TFObject) return Stream.of(single);
            throw new IllegalArgumentException("Unexpected data type: " + single);
        } else {
            System.out.format("DC2: %s%n", docContainers);
            DocContainer first = docContainers.get(0);
            DocContainer second = docContainers.get(1);

            if (second instanceof TFObject && first instanceof WordCounter) {
                DocContainer t = first;
                first = second;
                second = t;
            }

            if (first instanceof TFObject && second instanceof WordCounter) {
                TFObject tfObject = (TFObject)first;
                WordCounter wc = (WordCounter) second;

                System.out.format("DC22: %d%n", tfObject.idfSize());

                tfObject.addKey(wc.word(), wc.count());
                //System.out.format("====: %s %s %s%n", System.identityHashCode(tfObject), System.identityHashCode(newTF), newTF);
                return Stream.of(tfObject);
            }
            /*
            if (second instanceof TFObject) {
                TFObject tfObject = (TFObject)second;
                WordCounter wc = (WordCounter)first;
                return Stream.of(tfObject.addKey(wc.word(), wc.count()));
            }
            */
            /*
            if (first instanceof TFObject) {
                TFObject idf = (TFObject)first;
                WordCounter wc = (WordCounter) second;
                idf.add(wc.word(), wc.count());
                return Stream.of(first);
            }
            if (second instanceof IDFObject) {
                return Stream.of(second);
            }
*/

            throw new IllegalArgumentException("AAAAA");
//            WordCounter wc = (WordCounter) first;
//            return Stream.of(new TFObject(first.document(), wc.word(), wc.count()));


                //final WordCounter counter = (WordCounter) wordContainers.get(0);
                //return Stream.of(new WordCounter(counter.wordEntry(), counter.count() + 1));
        }
        //return Stream.of();
    }
}

