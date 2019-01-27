package com.spbsu.flamestream.example.bl.tfidfsd.ops.filtering;

import com.spbsu.flamestream.example.bl.tfidfsd.model.IDFObject;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class IDFObjectCompleteFilter implements Function<IDFObject, Stream<IDFObject>> {

    @Override
    public Stream<IDFObject> apply(IDFObject idfObject) {
        if (idfObject.isComplete()) {
            //System.out.println("11111111: " + idfObject);
            final IDFObject completed = new IDFObject(idfObject.document(), idfObject.idfCardinality(), idfObject.partitioning());
            for (Map.Entry<String, Integer> entry: idfObject.counts().entrySet()) {
                completed.counts().put(entry.getKey(), entry.getValue());
            }
            idfObject.counts().clear();
            return Stream.of(completed);
        } else {
            return Stream.empty();
        }
    }
}
