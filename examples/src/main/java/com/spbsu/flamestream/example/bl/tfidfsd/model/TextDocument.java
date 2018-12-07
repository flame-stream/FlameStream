package com.spbsu.flamestream.example.bl.tfidfsd.model;

import java.util.Objects;

public class TextDocument {
    private final String name;
    private final String content;

    public TextDocument(String name, String content) {
        this.name = name;
        this.content = content;
    }

    public String name() {
        return name;
    }

    public String content() {
        return content;
    }

    @Override
    public String toString() {
        return String.format("%s: >%s<", name, content);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TextDocument otherText = (TextDocument) o;
        return Objects.equals(name, otherText.name) && Objects.equals(content, otherText.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name.hashCode(), content.hashCode());
    }
}
