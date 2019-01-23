package com.spbsu.flamestream.example.bl.tfidfsd.ops.entries;

//import com.spbsu.flamestream.example.bl.tfidfsd.model.IDFObject;
//import com.spbsu.flamestream.example.bl.tfidfsd.model.IDFObject;
import com.spbsu.flamestream.example.bl.tfidfsd.model.IDFObject;
import com.spbsu.flamestream.example.bl.tfidfsd.model.TFObject;
import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidfsd.model.counters.DocCounter;
import com.spbsu.flamestream.example.bl.tfidfsd.model.counters.WordCounter;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class IDFAggregator implements Function<List<DocContainer>, Stream<DocContainer>> {
    @Override
    public Stream<DocContainer> apply(List<DocContainer> docContainers) {
        if (docContainers.size() == 1) {
            final WordCounter wordCounter = (WordCounter) docContainers.get(0);
            return Stream.of(new IDFObject(wordCounter.document(), wordCounter.word(), wordCounter.count(), wordCounter.idfCardinality(), wordCounter.partitioning()));
        } else {
            final IDFObject counter = (IDFObject) docContainers.get(0);
            final WordCounter wordCounter = (WordCounter) docContainers.get(1);
            IDFObject result = counter.merge(new IDFObject(wordCounter.document(), wordCounter.word(), wordCounter.count(), wordCounter.idfCardinality(), wordCounter.partitioning()));
            //System.out.println("RES: " + result);
            return Stream.of(result);
        }
    }
}

