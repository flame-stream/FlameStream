package com.spbsu.flamestream.example.bl.tfidfsd.model;

import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/*
public class IDFObject implements DocContainer {
    private final Map<String, Integer> counts;
    private final String docName;

    public IDFObject(String docName, String word, int count) {
        this.docName = docName;
        counts = new ConcurrentHashMap();
        counts.put(word, count);
    }

    public boolean isDefined(String key) {
        return counts.getOrDefault(key, 0) > 0;
    }

    public void add(String key, int value) {
        counts.put(key, value);
    }

    @Override
    public String document() {
        return docName;
    }

    @Override
    public String toString() {
        return String.format("%s", counts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final IDFObject otherIDFObject = (IDFObject) o;
        return Objects.equals(counts, otherIDFObject.counts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(counts.hashCode());
    }
}
*/