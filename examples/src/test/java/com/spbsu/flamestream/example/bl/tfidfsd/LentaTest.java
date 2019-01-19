package com.spbsu.flamestream.example.bl.tfidfsd;

import akka.japi.Pair;
import com.spbsu.flamestream.example.bl.tfidfsd.model.IDFData;
import com.spbsu.flamestream.example.bl.tfidfsd.model.TFData;
import com.spbsu.flamestream.example.bl.tfidfsd.model.TFObject;
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
import com.spbsu.flamestream.runtime.utils.QueuedConsumer;
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
    private Stream<TextDocument> documents() throws IOException {
        CSVParser csvFileParser = CSVParser.parse(
                new File("/Users/sergeyreznick/Downloads/lenta-ru-news.csv"), Charset.defaultCharset(), CSVFormat.DEFAULT
        );
        Iterator<CSVRecord> iter = csvFileParser.iterator();
        iter.next();
        Stream<CSVRecord> stream = Stream.generate(() -> iter.next());
        Stream<CSVRecord> records = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(csvFileParser.iterator(), Spliterator.IMMUTABLE), false);

        return records.map(r -> {
            Pattern p = Pattern.compile("\\w+", Pattern.UNICODE_CHARACTER_CLASS);
            Matcher m = p.matcher(r.get(2));
            StringBuilder text = new StringBuilder();
            while (m.find()) {
                text.append(" ");
                text.append(m.group());
            }
            return new TextDocument(r.get(0), text.substring(1));
        }).limit(2048);
    }

    @Test
    public void lentaTest() throws InterruptedException, IOException {
        int nExpected = 2048
                ;
        final /*AwaitResultConsumer*/ QueuedConsumer<Object> awaitConsumer = new QueuedConsumer<>(nExpected);

        Stream<TextDocument> toCheck = documents();

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

            System.out.println("1111111111");
            Thread t = new Thread(() -> {
                Iterator<TextDocument> toCheckIter = toCheck.iterator();
                IDFData idfExpected = new IDFData();
                Queue q = awaitConsumer.queue();
                for (int i = 0; i < nExpected; i++) {
                    Object o = q.poll();
                    while (o == null){
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {}
                        o = q.poll();
                    }
                    if (o instanceof TFObject) {
                        TextDocument processedDoc = toCheckIter.next();
                        List<String> pdWords = TextUtils.words(processedDoc.content());
                        idfExpected.addWords(new HashSet(pdWords));
                        System.out.format("pdWords: %d %d %d %s%n", pdWords.size(), idfExpected.keys().size(), i, pdWords);
                        TFData pdTF = TextUtils.tfData(processedDoc.content());
                        TFObject tfoResult = (TFObject) o;

                        Assert.assertEquals(processedDoc.name(), tfoResult.document());
                        Assert.assertEquals(pdTF.keys(), tfoResult.tfKeys());
                        for (String key: pdTF.keys()) {
                            Assert.assertEquals(pdTF.value(key), tfoResult.tfCount(key));
                        }
                        Assert.assertEquals(pdTF.keys(), tfoResult.idfKeys());
                        for (String key: pdTF.keys()) {
                            Assert.assertEquals(idfExpected.value(key), tfoResult.idfCount(key));
                        }
                    } else {
                        System.out.println("unexpected: " + o);
                    }
                }
                System.out.println("DONE");
            });

            t.start();
            System.out.println("2222222222");

            //awaitConsumer.await(500, TimeUnit.MINUTES);

            documents().forEach(front);

            t.join();
        }
    }
}
