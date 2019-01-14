package com.spbsu.flamestream.example.bl.tfidfsd.model;

import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;


public class IDFObject implements DocContainer {
    private final Map<String, Integer> counts = new ConcurrentHashMap<>();
    private final String docName;
    private AtomicBoolean selfGrouped;

    public IDFObject(String docName, String word, Integer count) {
        this.docName = docName;
        counts.put(word, count);
        selfGrouped = new AtomicBoolean(false);
    }

    private IDFObject(String docName) {

        this.docName = docName;
        selfGrouped = new AtomicBoolean(false);
    }

    public IDFObject merge(IDFObject other) {
        IDFObject result = new IDFObject(docName);
        for (Map.Entry<String, Integer> entry: other.counts.entrySet()) {
            result.counts.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, Integer> entry: counts.entrySet()) {
            result.counts.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public boolean isDefined(String key) {
        return counts.containsKey(key);
    }

    public boolean isSelfGrouped() {
        return selfGrouped.get();
    }

    public void setSelfGrouped() {
        selfGrouped.set(true);
    }

    public void drop(String key) {
        counts.remove(key);
    }

    @Override
    public String document() {
        return docName;
    }

    @Override
    public String toString() {
        return String.format("<IDFO> %s", counts);
    }

    public Set<String> keys() {
        return counts.keySet();
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