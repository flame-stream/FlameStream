package com.spbsu.flamestream.example.bl.tfidfsd.ops.filtering;

import com.spbsu.flamestream.example.bl.tfidfsd.model.IDFObject;
import com.spbsu.flamestream.example.bl.tfidfsd.model.TFObject;
import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class TfIdfFilter implements Function<List<DocContainer>, Stream<DocContainer>> {

    @Override
    public Stream<DocContainer> apply(List<DocContainer> elements) {
        if (elements.size() < 2) {
            return Stream.of();
        } else {
            if (elements.get(0) instanceof TFObject && elements.get(1) instanceof  IDFObject) {
                elements = Arrays.asList(elements.get(1), elements.get(0));
            }
            IDFObject idf = (IDFObject)elements.get(0);
            TFObject tf = (TFObject)elements.get(1);
            TFObject res = tf.withIdf(idf.counts());
            //System.out.println("333333: " + res);
            return Stream.of(res);
        }

        //if (idfObject.isComplete()) {
        //    System.out.println("11111111: " + idfObject);
        //    return Stream.of(idfObject);
        //} else {
        //    return Stream.empty();
        //}
    }
}
