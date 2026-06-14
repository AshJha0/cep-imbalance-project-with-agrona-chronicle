package com.example.cep;

import com.example.cep.core.ImbalanceProcessor;
import com.example.cep.core.ImbalanceProcessor.AlertSink;
import com.example.cep.pipeline.DisruptorPipeline;
import com.example.cep.pipeline.AgronaSpscPipeline;
import com.example.cep.pipeline.ChronicleQueuePipeline;
import com.example.cep.load.MultiSymbolLoadGen;
import com.example.cep.util.LatencyRecorder;

public class App {

    public static void main(String[] args) throws Exception {
        double threshold = 0.60;   // 60%
        double hysteresis = 0.55;  // 55%
        long minTotal = 1_000;
        long sustainMs = 50;

        AlertSink sink = new AlertSink() {
            @Override
            public void onImbalanceAlert(String symbol, double imbalance, long bid, long ask, long tsNanos) {
                System.out.printf("ALERT  %s  imbalance=%.2f%%  bid=%d ask=%d%n",
                    symbol, 100.0*imbalance, bid, ask);
            }
            @Override
            public void onImbalanceCleared(String symbol, long tsNanos) {
                System.out.printf("CLEAR  %s  back within band%n", symbol);
            }
        };

        ImbalanceProcessor processor = new ImbalanceProcessor(threshold, hysteresis, minTotal, sustainMs, sink);

        // latency recorder - max 1 second in microseconds, 3 sig digits
        LatencyRecorder recorder = new LatencyRecorder(1_000_000L, 3);

        // Choose pipeline: Disruptor, Agrona SPSC, or Chronicle Queue
        DisruptorPipeline disruptor = new DisruptorPipeline(processor, recorder);
        AgronaSpscPipeline agrona = new AgronaSpscPipeline(14, processor, recorder); // 2^14 slots
        ChronicleQueuePipeline chronicle = new ChronicleQueuePipeline("chronicle-queue-data", processor, recorder);

        // Start disruptor
        disruptor.start();

        // Multi-symbol load generator publishes into the pipelines; change target as desired
        MultiSymbolLoadGen gen = new MultiSymbolLoadGen(8, 50_000, q -> {
            disruptor.publish(q);
            agrona.accept(q);
            chronicle.accept(q);
        });

        gen.runForMillis(2000);

        // allow a short drain and stop
        Thread.sleep(500);
        disruptor.stop();
        agrona.stop();
        chronicle.stop();

        // Print latency summary
        recorder.printSummary();

        System.out.println("Done.");
    }
}
