package com.example.cep.pipeline;

import com.example.cep.core.ImbalanceProcessor;
import com.example.cep.model.QuoteUpdate;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.agrona.concurrent.BusySpinIdleStrategy;
import com.example.cep.util.LatencyRecorder;

import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

public class DisruptorPipeline implements Consumer<QuoteUpdate> {

    private static final int RING_SIZE = 1 << 16; // 65,536
    private final Disruptor<QuoteEvent> disruptor;
    private final RingBuffer<QuoteEvent> ringBuffer;
    private final LatencyRecorder recorder;

    public DisruptorPipeline(ImbalanceProcessor processor, LatencyRecorder recorder) {
        this.recorder = recorder;
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "disruptor-consumer");
            t.setDaemon(true);
            return t;
        };

        disruptor = new Disruptor<>(QuoteEvent::new, RING_SIZE, tf,
                ProducerType.MULTI,
                new BusySpinWaitStrategy()
        );

        EventHandler<QuoteEvent> handler = (event, sequence, endOfBatch) -> {
            long now = System.nanoTime();
            if (event.value != null) {
                processor.onQuote(event.value);
                // record latency (microseconds)
                if (recorder != null) {
                    long latencyUs = Math.max(0, (now - event.value.tsNanos()) / 1000L);
                    recorder.recordValue(latencyUs);
                }
            }
            event.value = null;
        };

        disruptor.handleEventsWith(handler);
        ringBuffer = disruptor.getRingBuffer();
    }

    public void start() { disruptor.start(); }

    public void stop() { disruptor.shutdown(); }

    public void publish(QuoteUpdate q) {
        long seq = ringBuffer.next();
        try {
            QuoteEvent evt = ringBuffer.get(seq);
            evt.value = q;
        } finally {
            ringBuffer.publish(seq);
        }
    }

    @Override
    public void accept(QuoteUpdate quoteUpdate) {
        publish(quoteUpdate);
    }

    public static class QuoteEvent {
        public QuoteUpdate value;
    }
}
