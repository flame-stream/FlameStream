package com.spbsu.flamestream.example.bl.tfidfsd;

import com.spbsu.flamestream.example.bl.tfidfsd.model.IDFData;
import com.spbsu.flamestream.example.bl.tfidfsd.model.TFData;
import com.spbsu.flamestream.example.bl.tfidfsd.model.TFObject;
import com.spbsu.flamestream.example.bl.tfidfsd.model.TextDocument;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.QueuedConsumer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class SmallDSTest {
    private static final int SIZE = 50;

    private Stream<TextDocument> data01() {
        return IntStream.range(0, SIZE).mapToObj(n -> new TextDocument("doc" + n, "a"));
    }

    @Test
    public void test01() throws InterruptedException, IOException {
        int nExpected = SIZE;
        final QueuedConsumer<Object> consumer = new QueuedConsumer<>(nExpected);

        Stream<TextDocument> toCheck = data01();

        try (final LocalRuntime runtime = new LocalRuntime.Builder().maxElementsInGraph(2)
                .millisBetweenCommits(500)
                .build()) {

            final FlameRuntime.Flame flame = runtime.run(new TfIdfGraphSD().get());

            flame.attachRear("tfidfRear", new AkkaRearType<>(runtime.system(), Object.class))
                    .forEach(r -> r.addListener(consumer));
            final List<AkkaFront.FrontHandle<TextDocument>> handles = flame
                    .attachFront("tfidfFront", new AkkaFrontType<TextDocument>(runtime.system()))
                    .collect(toList());

            final AkkaFront.FrontHandle<TextDocument> front = handles.get(0);
            for (int i = 1; i < handles.size(); i++) {
                handles.get(i).unregister();
            }

            Thread t = new Thread(() -> {
                Iterator<TextDocument> toCheckIter = toCheck.iterator();
                IDFData idfExpected = new IDFData();
                Queue q = consumer.queue();
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

            data01().forEach(front);

            t.join();
        }
    }

    public void worker(Supplier<Stream<TextDocument>> docsSource) throws InterruptedException, IOException {
        int nExpected = 1;
        final QueuedConsumer<Object> consumer = new QueuedConsumer<>(nExpected);

        Stream<TextDocument> toCheck = docsSource.get();

        try (final LocalRuntime runtime = new LocalRuntime.Builder().maxElementsInGraph(2)
                .millisBetweenCommits(500)
                .build()) {

            final FlameRuntime.Flame flame = runtime.run(new TfIdfGraphSD().get());

            flame.attachRear("tfidfRear", new AkkaRearType<>(runtime.system(), Object.class))
                    .forEach(r -> r.addListener(consumer));
            final List<AkkaFront.FrontHandle<TextDocument>> handles = flame
                    .attachFront("tfidfFront", new AkkaFrontType<TextDocument>(runtime.system()))
                    .collect(toList());

            final AkkaFront.FrontHandle<TextDocument> front = handles.get(0);
            for (int i = 1; i < handles.size(); i++) {
                handles.get(i).unregister();
            }

            Thread t = new Thread(() -> {
                Iterator<TextDocument> toCheckIter = toCheck.iterator();
                IDFData idfExpected = new IDFData();
                Queue q = consumer.queue();
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

            docsSource.get().forEach(front);

            t.join();
        }
    }
}