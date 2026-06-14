package com.example.cep;

import com.example.cep.core.ImbalanceProcessor;
import com.example.cep.model.QuoteUpdate;
import com.example.cep.model.Side;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class ImbalanceProcessorTest {

    @Test
    void triggersAndClearsWithSustainAndHysteresis() {
        AtomicBoolean alerted = new AtomicBoolean(false);
        AtomicBoolean cleared = new AtomicBoolean(false);

        ImbalanceProcessor.AlertSink sink = new ImbalanceProcessor.AlertSink() {
            @Override public void onImbalanceAlert(String symbol, double imbalance, long bid, long ask, long ts) {
                alerted.set(true);
            }
            @Override public void onImbalanceCleared(String symbol, long ts) {
                cleared.set(true);
            }
        };

        ImbalanceProcessor proc = new ImbalanceProcessor(0.6, 0.55, 1000, 50, sink);
        String sym = "XS1";
        long t0 = System.nanoTime();

        // Balanced
        proc.onQuote(new QuoteUpdate(sym, Side.BID, 600, t0));
        proc.onQuote(new QuoteUpdate(sym, Side.ASK, 650, t0));

        // Cross threshold
        proc.onQuote(new QuoteUpdate(sym, Side.BID, 5000, t0 + 10_000_000)); // 10ms later
        // sustain not met yet
        assertFalse(alerted.get());

        // After sustain
        proc.onQuote(new QuoteUpdate(sym, Side.ASK, 1200, t0 + 70_000_000)); // +70ms
        assertTrue(alerted.get());

        // Clear with small imbalance
        proc.onQuote(new QuoteUpdate(sym, Side.BID, 1400, t0 + 120_000_000));
        assertTrue(cleared.get());
    }
}
