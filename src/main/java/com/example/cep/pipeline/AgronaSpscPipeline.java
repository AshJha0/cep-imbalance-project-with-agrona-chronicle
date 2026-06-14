package com.example.cep.pipeline;

import com.example.cep.core.ImbalanceProcessor;
import com.example.cep.model.QuoteUpdate;
import com.example.cep.util.LatencyRecorder;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Lightweight SPSC ring buffer (Agrona-style semantics) for demo purposes.
 * Single publisher thread, single consumer thread.
 */
public class AgronaSpscPipeline implements Consumer<QuoteUpdate> {
    private final QuoteUpdate[] buffer;
    private final int mask;
    private final AtomicLong head = new AtomicLong(0);
    private final AtomicLong tail = new AtomicLong(0);
    private final Thread consumerThread;
    private volatile boolean running = true;
    private final ImbalanceProcessor processor;
    private final LatencyRecorder recorder;

    public AgronaSpscPipeline(int capacityPow2, ImbalanceProcessor processor, LatencyRecorder recorder) {
        this.buffer = new QuoteUpdate[1 << capacityPow2];
        this.mask = buffer.length - 1;
        this.processor = processor;
        this.recorder = recorder;

        consumerThread = new Thread(this::consumerLoop, "agrona-spsc-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    private void consumerLoop() {
        while (running) {
            long h = head.get();
            long t = tail.get();
            if (h < t) {
                int idx = (int)(h & mask);
                QuoteUpdate q = buffer[idx];
                buffer[idx] = null;
                head.lazySet(h + 1);
                if (q != null) {
                    long now = System.nanoTime();
                    processor.onQuote(q);
                    if (recorder != null) recorder.recordValue((now - q.tsNanos()) / 1000L);
                }
            } else {
                Thread.yield();
            }
        }
    }

    @Override
    public void accept(QuoteUpdate q) {
        long t = tail.get();
        int idx = (int)(t & mask);
        buffer[idx] = q;
        // publish
        tail.lazySet(t + 1);
    }

    public void stop() {
        running = false;
        try {
            consumerThread.join(1000);
        } catch (InterruptedException ignored) {}
    }
}
