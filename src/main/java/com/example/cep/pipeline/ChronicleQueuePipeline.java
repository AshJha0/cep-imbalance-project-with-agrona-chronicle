package com.example.cep.pipeline;

import com.example.cep.core.ImbalanceProcessor;
import com.example.cep.model.QuoteUpdate;
import com.example.cep.util.LatencyRecorder;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;

import java.io.File;
import java.util.function.Consumer;

/**
 * Chronicle Queue based publisher/consumer for persisted low-latency pipeline.
 * Note: This is a simplified demo usage; production code should manage queue lifecycle and file paths.
 */
public class ChronicleQueuePipeline implements Consumer<QuoteUpdate> {

    private final ChronicleQueue queue;
    private final Thread readerThread;
    private volatile boolean running = true;
    private final LatencyRecorder recorder;
    private final ImbalanceProcessor processor;

    public ChronicleQueuePipeline(String path, ImbalanceProcessor processor, LatencyRecorder recorder) {
        this.queue = ChronicleQueue.singleBuilder(new File(path))
                .wireType(WireType.TEXT)
                .build();
        this.processor = processor;
        this.recorder = recorder;
        this.readerThread = new Thread(this::readerLoop, "chronicle-reader");
        readerThread.setDaemon(true);
        readerThread.start();
    }

    @Override
    public void accept(QuoteUpdate q) {
        try (DocumentContext dc = queue.acquireAppender().writingDocument()) {
            Wire w = dc.wire();
            w.write("symbol").text(q.symbol());
            w.write("side").text(q.side().name());
            w.write("size").int64(q.size());
            w.write("ts").int64(q.tsNanos());
        } catch (Exception ex) {
            // ignore for demo
        }
    }

    private void readerLoop() {
        ExcerptTailer tailer = queue.createTailer();
        while (running) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent()) {
                    Thread.yield();
                    continue;
                }
                Wire in = dc.wire();
                String symbol = in.read("symbol").text();
                String side = in.read("side").text();
                long size = in.read("size").int64();
                long ts = in.read("ts").int64();

                QuoteUpdate q = new QuoteUpdate(symbol, com.example.cep.model.Side.valueOf(side), size, ts);
                long now = System.nanoTime();
                processor.onQuote(q);
                if (recorder != null) recorder.recordValue((now - ts) / 1000L);
            } catch (Exception ex) {
                Thread.yield();
            }
        }
    }

    public void stop() {
        running = false;
        try {
            readerThread.join(1000);
        } catch (InterruptedException ignored) {}
        queue.close();
    }
}
