package com.spbsu.flamestream.example.bl.tfidfsd;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.example.bl.tfidfsd.model.TextDocument;
import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.WordContainer;
import com.spbsu.flamestream.example.bl.tfidfsd.model.counters.DocCounter;
import com.spbsu.flamestream.example.bl.tfidfsd.model.counters.WordCounter;
import com.spbsu.flamestream.example.bl.tfidfsd.model.counters.WordDocCounter;
import com.spbsu.flamestream.example.bl.tfidfsd.model.entries.DocEntry;
import com.spbsu.flamestream.example.bl.tfidfsd.model.entries.WordDocEntry;
import com.spbsu.flamestream.example.bl.tfidfsd.model.entries.WordEntry;
import com.spbsu.flamestream.example.bl.tfidfsd.ops.entries.CountWordEntries;
import com.spbsu.flamestream.example.bl.tfidfsd.ops.filtering.WordContainerOrderingFilter;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class TfIdfGraphSD implements Supplier<Graph> {
    private final HashFunction wordHash = HashFunction.uniformHash(HashFunction.objectHash(WordContainer.class));

    @SuppressWarnings("Convert2Lambda")
    private final Equalz equalzWord = new Equalz() {
        @Override
        public boolean test(DataItem o1, DataItem o2) {
            return o1.payload(WordContainer.class).word().equals(o2.payload(WordContainer.class).word());
        }
    };

    @Override
    public Graph get() {
        final Source source = new Source();
        final Grouping<WordContainer> groupingWord2 =
                new Grouping<>(wordHash, equalzWord, 2, WordContainer.class);

        final FlameMap<TextDocument, WordDocCounter> splitterWordDoc2 = new FlameMap<>(new Function<TextDocument, Stream<WordDocCounter>>() {
            private Pattern p = Pattern.compile("\\w+", Pattern.UNICODE_CHARACTER_CLASS);
            //private final Pattern pattern = Pattern.compile("\\w+");
            AtomicInteger docs = new AtomicInteger();
            AtomicInteger wdocs = new AtomicInteger();

            @Override
            public Stream<WordDocCounter> apply(TextDocument s) {
                final Map<String, Integer> counter = new HashMap<>();
                Matcher m = p.matcher(s.content());
                int nd = docs.incrementAndGet();


  //              System.out.format("doc %s >%s<%n", s.name(), s.content());
                while (m.find()) {
                    String w = m.group();
                    counter.put(w, counter.getOrDefault(w, 0) + 1);
//                    System.out.format("doc %s =%s=%n", s.name(), w);
                }

                int nwd = wdocs.addAndGet(counter.size());

                System.out.format("nd: %d, mwd: %d%n", nd, nwd);



                return counter.entrySet().stream()
                        .map(word -> new WordDocCounter(new WordDocEntry(s.name(), word.getKey()), word.getValue()));
            }
        }, TextDocument.class);  /*new FlameMap<>(new Function<TextDocument, Stream<WordDocCounter>>() {
            private final Pattern pattern = Pattern.compile("\\s");

            @Override
            public Stream<WordDocCounter> apply(TextDocument s) {
                final Map<String, Integer> counter = new HashMap<>();
                for (String w: pattern.split(s.content())) {
                    counter.put(w, counter.getOrDefault(w, 0) + 1);
                }
                System.out.format("doc %s%n", s.name());
                return counter.entrySet().stream()
                        .map(word -> new WordDocCounter(new WordDocEntry(s.name(), word.getKey()), word.getValue()));
            }
        }, TextDocument.class);*/

        final FlameMap<TextDocument, DocCounter> splitterDoc2 = new FlameMap<>(new Function<TextDocument, Stream<DocCounter>>() {
            private final Pattern pattern = Pattern.compile("\\s");

            @Override
            public Stream<DocCounter> apply(TextDocument s) {
                return Stream.of(new DocCounter(new DocEntry(s.name()),pattern.split(s.content()).length));
            }
        }, TextDocument.class);

        final FlameMap<TextDocument, WordEntry> splitterWord2 = new FlameMap<>(new Function<TextDocument, Stream<WordEntry>>() {
            private final Pattern pattern = Pattern.compile("\\s");

            @Override
            public Stream<WordEntry> apply(TextDocument s) {
                final Map<String, Integer> counter = new HashMap<>();
                for (String w : pattern.split(s.content())) {
                    counter.put(w, counter.getOrDefault(w, 0) + 1);
                }
                return counter.entrySet().stream()
                        .map(word -> new WordEntry(word.getKey()));
            }
        }, TextDocument.class);

        final FlameMap<List<WordContainer>, List<WordContainer>> filterWord2 = new FlameMap<>(
                new WordContainerOrderingFilter(),
                List.class
        );
        final FlameMap<List<WordContainer>, WordCounter> counterWord2 = new FlameMap<>(
                new CountWordEntries(),
                List.class
        );

        final Sink sink = new Sink();
        return new Graph.Builder()
                .link(source, splitterWordDoc2)
                .link(source, splitterDoc2)
                .link(source, splitterWord2)
                .link(splitterWord2, groupingWord2)
                .link(groupingWord2, filterWord2)
                .link(filterWord2, counterWord2)
                .link(counterWord2, groupingWord2)

                .colocate(source, splitterWordDoc2, splitterDoc2)
                .colocate(groupingWord2, filterWord2, counterWord2, sink)

                .link(splitterWordDoc2, sink)
                .link(splitterDoc2, sink)
  //              .link(counterWord2, sink)


                .build(source, sink);
    }
}
