package com.spbsu.flamestream.example.bl.tfidfsd.model;

import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TFObject implements DocContainer {
    private final Map<String, Integer> counts;
    private final Map<String, Integer> idfCounts;
    private final String docName;
    private final int idfCardinality;

    public Set<String> tfKeys() {
        return counts.keySet();
    }
    public Set<String> idfKeys() {
        return idfCounts.keySet();
    }

    public int tfCount(String key) {
        return counts.get(key);
    }

    public int idfCount(String key) {
        return idfCounts.get(key);
    }

    public TFObject(String docName, String words[]) {
        this.docName = docName;
        counts = new ConcurrentHashMap();
        idfCounts = new ConcurrentHashMap();
        for (String s: words) {
            counts.put(s, counts.getOrDefault(s, 0) + 1);
        }
        idfCardinality = 0;
    }

    public TFObject withIdf(Map idf) {
        return new TFObject(docName, counts, idf);
    }

    public TFObject(TFObject other) {
        this.docName = other.docName;
        this.counts = other.counts;
        this.idfCounts = new ConcurrentHashMap<>(other.idfCounts);
        idfCardinality = 0;
    }

    public TFObject(String docName, Map counts, Map idfCounts) {
        this.docName = docName;
        this.counts = counts;
        this.idfCounts = idfCounts;
        idfCardinality = 0;
    }

    public boolean isDefinedIdf(String key) {
        return idfCounts.getOrDefault(key, 0) > 0;
    }

    public int idfSize() {
        return idfCounts.size();
    }


    public void addKey(String key) {
        //TFObject result = new TFObject(this);
        //result.
                idfCounts.put(key, idfCounts.getOrDefault(key, 0) + 1);
        //return result;
    }

    @Override
    public String document() {
        return docName;
    }

    @Override
    public int idfCardinality() {
        return idfCardinality;
    }

    @Override
    public String toString() {
        return String.format("<TFO> doc hash: %d, doc: %s, idf: %s, words: %s", docName.hashCode(), docName, idfCounts, counts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TFObject otherTFObject = (TFObject) o;
        return Objects.equals(counts, otherTFObject.counts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(counts.hashCode());
    }
}
