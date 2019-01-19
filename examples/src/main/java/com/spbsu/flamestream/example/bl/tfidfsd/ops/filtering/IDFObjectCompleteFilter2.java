package com.spbsu.flamestream.example.bl.tfidfsd.ops.filtering;

import com.spbsu.flamestream.example.bl.tfidfsd.model.IDFObject;

import java.util.function.Function;
import java.util.stream.Stream;

public class IDFObjectCompleteFilter2 implements Function<IDFObject, Stream<IDFObject>> {

    @Override
    public Stream<IDFObject> apply(IDFObject idfObject) {
        if (idfObject.isComplete()) {
            //System.out.println("11111111: " + idfObject);
            return Stream.of();
        } else {
            return Stream.of(idfObject);
        }
    }
}
