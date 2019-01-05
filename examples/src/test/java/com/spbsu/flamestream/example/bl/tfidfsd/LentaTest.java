package com.spbsu.flamestream.example.bl.tfidfsd;

import akka.japi.Pair;
import com.spbsu.flamestream.example.bl.tfidfsd.model.TextDocument;
import com.spbsu.flamestream.example.bl.tfidfsd.model.counters.DocCounter;
import com.spbsu.flamestream.example.bl.tfidfsd.model.counters.WordCounter;
import com.spbsu.flamestream.example.bl.tfidfsd.model.counters.WordDocCounter;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.*;

public class LentaTest extends FlameAkkaSuite {
    private Stream<Pair<String, String>> wordDocPairs(Stream<TextDocument> docs) {
        final Pattern pattern = Pattern.compile("\\s");
        return docs.flatMap(doc -> Arrays.stream(pattern.split(doc.content()))
                        .map(word -> new Pair<>(word, doc.name())));
    }

    private Stream<TextDocument> documents() throws IOException {
        CSVParser csvFileParser = CSVParser.parse(
                new File("C:/work/projects/2/FlameStream/news_lenta.csv"), Charset.defaultCharset(), CSVFormat.DEFAULT
        );
        Iterator<CSVRecord> iter = csvFileParser.iterator();
        iter.next();
        Stream<CSVRecord> stream = Stream.generate(() -> iter.next());
        Stream<CSVRecord> records = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(csvFileParser.iterator(), Spliterator.IMMUTABLE), false);

        return records.map(r -> {
            Pattern p = Pattern.compile("\\w+", Pattern.UNICODE_CHARACTER_CLASS);
            Matcher m = p.matcher(r.get(1));
            StringBuilder text = new StringBuilder();
            while (m.find()) {
                text.append(" ");
                text.append(m.group());
            }
            return new TextDocument(r.get(4), text.substring(1));
        }).limit(1);
    }

    @Test
    public void lentaTest() throws InterruptedException, IOException {
        Stream<TextDocument> input = documents();
        final Pattern pattern = Pattern.compile("\\s");

        /*final Map<Pair<String, String>, Integer> expectedWordDoc = new ConcurrentHashMap<>(); */  /* = wordDocPairs(input)
                .collect(groupingBy(
                        Pair::first,
                        mapping(Pair::second, toMap(Function.identity(), o -> 1, Integer::sum))
                ));
*/

        /*
        System.out.println("AAAAA");
        final Map<String, Integer> expectedDoc = input
                .flatMap(doc -> Arrays.stream(pattern.split(doc.content()))
                        .map(word -> new Pair<>(doc.name(), word)))
                .collect(toMap(Pair::first, o -> 1, Integer::sum));
        System.out.println(expectedDoc);
        final Map<String, Integer> expectedIdf = wordDocPairs(documents())
                .collect(groupingBy(Pair::first, collectingAndThen(toSet(), Set::size)));
        System.out.println("AAAAA: " + expectedIdf);
*/

//        ConcurrentHashMap<String, Integer> docCards = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Integer> idf = new ConcurrentHashMap<>();
        AtomicInteger nWordDocs = new AtomicInteger(0);
        documents().forEach(textDoc -> {
            Pattern p = Pattern.compile("\\s");
            Set<String> wset = new HashSet<>();
            String words[] = p.split(textDoc.content());
            for (String s: words)  {
                Pair<String, String> wd = new Pair<>(s, textDoc.name());
//                expectedWordDoc.put(wd, expectedWordDoc.getOrDefault(wd, 0) + 1);
                wset.add(s);
            }
            for (String s: wset) {
                int v = idf.getOrDefault(s, 0);
                wset.add(s);
                idf.put(s, v + 1);
            }
            nWordDocs.addAndGet(wset.size());
  //          docCards.put(textDoc.name(), words.length);
        });

    //    int nExpectedDocCards = docCards.size();
        int nExpected = /*docCards.size() + expectedWordDoc.size() + */nWordDocs.get() + 1;
        System.out.println("AAA: " + nExpected);
        final AwaitResultConsumer<Object> awaitConsumer = new AwaitResultConsumer<>(nExpected + 1 + 1 + 1000000);
        try (final LocalRuntime runtime = new LocalRuntime.Builder().maxElementsInGraph(2)
                .millisBetweenCommits(500)
                .build()) {

            final FlameRuntime.Flame flame = runtime.run(new TfIdfGraphSD().get());

            flame.attachRear("tfidfRear", new AkkaRearType<>(runtime.system(), Object.class))
                    .forEach(r -> r.addListener(awaitConsumer));
            final List<AkkaFront.FrontHandle<TextDocument>> handles = flame
                    .attachFront("tfidfFront", new AkkaFrontType<TextDocument>(runtime.system()))
                    .collect(toList());

            final AkkaFront.FrontHandle<TextDocument> front = handles.get(0);
            for (int i = 1; i < handles.size(); i++) {
                handles.get(i).unregister();
            }

           // ConcurrentLinkedQueue<TextDocument> q = new ConcurrentLinkedQueue<>();
           // q.addAll(documents().collect(toList()));
           // applyDataToAllHandlesAsync(q, handles);

            Stream<TextDocument> input2 = documents();
            Iterable<TextDocument> docs = input2::iterator;
            //applyDataToHandleAsync(documents(), handles.get(0));

//            docs.iterator();
            documents().forEach(front);
            awaitConsumer.await(500, TimeUnit.MINUTES);

            final Map<String, Integer> actualDoc = new HashMap<>();
            final Map<String, Integer> actualIdf = new HashMap<>();
            final Map<Pair<String, String>, Integer> actualPairs = new HashMap<>();
            awaitConsumer.result().forEach(o -> {
                System.out.println("GOT" + o);
                if (o instanceof WordDocCounter) {
         //           System.out.println("GOT WDC " + o);
                    final WordDocCounter wordDocCounter = (WordDocCounter) o;
                    Pair<String, String> p = new Pair<>(wordDocCounter.word(), wordDocCounter.document());
                    actualPairs.putIfAbsent(p, 0);
                    actualPairs.computeIfPresent(p, (uid, old) -> Math.max(wordDocCounter.count(), old));
                } else if (o instanceof DocCounter) {
           //         System.out.println("GOT DC " + o);
                    final DocCounter docCounter = (DocCounter) o;
                    actualDoc.putIfAbsent(docCounter.document(), 0);
                    actualDoc.computeIfPresent(docCounter.document(), (uid, old) -> Math.max(docCounter.count(), old));
                } else if (o instanceof WordCounter) {
                    System.out.println("GOT WC " + o);
                    final WordCounter wordCounter = (WordCounter) o;
                    actualIdf.putIfAbsent(wordCounter.word(), 0);
                    actualIdf.computeIfPresent(wordCounter.word(), (uid, old) -> Math.max(wordCounter.count(), old));
                } else {
                    System.out.println("unexpected: " + o);
                }
            });
            //Assert.assertEquals(actualDoc, docCards);
            //System.out.println("idf: " + idf);
            //Assert.assertEquals(actualIdf, idf);
            //Assert.assertEquals(actualPairs, expectedWordDoc);
        }


        /*
        final Map<String, Integer> actualDoc = new HashMap<>();
        final Map<String, Integer> actualIdf = new HashMap<>();
        final Map<Pair<String, String>, Integer> actualPairs = new HashMap<>();
        awaitConsumer.result().forEach(o -> {
            if (o instanceof WordDocCounter) {
                final WordDocCounter wordDocCounter = (WordDocCounter) o;
                Pair<String, String> p = new Pair<>(wordDocCounter.word(), wordDocCounter.document());
                actualPairs.putIfAbsent(p, 0);
                actualPairs.computeIfPresent(p, (uid, old) -> Math.max(wordDocCounter.count(), old));
            } else if (o instanceof DocCounter) {
                final DocCounter docCounter = (DocCounter) o;
                actualDoc.putIfAbsent(docCounter.document(), 0);
                actualDoc.computeIfPresent(docCounter.document(), (uid, old) -> Math.max(docCounter.count(), old));
            } else if (o instanceof WordCounter) {
                final WordCounter wordCounter = (WordCounter) o;
                actualIdf.putIfAbsent(wordCounter.word(), 0);
                actualIdf.computeIfPresent(wordCounter.word(), (uid, old) -> Math.max(wordCounter.count(), old));
            } else {
                System.out.println("unexpected: " + o);
            }
        });

        final Map<String, Map<String, Integer>> actualWordDoc = actualPairs.entrySet().stream()
                .collect(groupingBy(
                        v -> v.getKey().first(),
                        toMap(v -> v.getKey().second(), Map.Entry::getValue)
                ));

        Assert.assertEquals(actualWordDoc, expectedWordDoc);
        Assert.assertEquals(actualDoc, expectedDoc);
        Assert.assertEquals(actualIdf, expectedIdf);
        */
    }
}
