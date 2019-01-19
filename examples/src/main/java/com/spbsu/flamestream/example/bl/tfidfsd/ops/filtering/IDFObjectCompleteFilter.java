package com.spbsu.flamestream.example.bl.tfidfsd.ops.filtering;

import com.spbsu.flamestream.example.bl.tfidfsd.model.IDFObject;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class IDFObjectCompleteFilter implements Function<IDFObject, Stream<IDFObject>> {

    @Override
    public Stream<IDFObject> apply(IDFObject idfObject) {
        if (idfObject.isComplete()) {
            //System.out.println("11111111: " + idfObject);
            return Stream.of(idfObject);
        } else {
            return Stream.empty();
        }
    }
}
