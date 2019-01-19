package com.spbsu.flamestream.example.bl.tfidfsd.model;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class IDFData {
    private Map<String, Integer> data;

    public IDFData() {
        data = new ConcurrentHashMap<>();
    }

    public void addWords(Collection<String> words) {
        for (String w: words) {
            data.put(w, data.getOrDefault(w, 0) + 1);
        }
    }

    public Set<String> keys() {
        return data.keySet();
    }

    public int value(String key) {
        return data.get(key);
    }
}
