package com.spbsu.datastream.example.invertedindex.io;

import akka.util.BoundedBlockingQueue;
import com.spbsu.commons.func.Processor;
import com.spbsu.datastream.example.invertedindex.models.WikiPage;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by nuka3 on 3/2/17.
 */
public class WikiForiaPageIterator implements Iterator<WikiPage>, Processor<WikiPage>{
    private WikiPage next;
    BlockingQueue<WikiPage> queue;
    private WikiforiaParser parser;

    public WikiForiaPageIterator(InputStream inputStream) {
        parser = new WikiforiaParser();
        queue = new BoundedBlockingQueue<>(100, new ArrayDeque<>());
        parser.setProcessor(this);
        try {
            parser.parse(inputStream);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNext() {
        try {
            next = queue.poll(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return next != null;
    }

    @Override
    public WikiPage next() {
        return next;
    }

    @Override
    public void process(WikiPage page) {
        queue.add(page);
    }
}
