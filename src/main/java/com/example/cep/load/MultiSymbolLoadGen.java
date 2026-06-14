package com.example.cep.load;

import com.example.cep.model.QuoteUpdate;
import com.example.cep.model.Side;

import java.util.Random;
import java.util.function.Consumer;

/**
 * Synthetic multi-symbol publisher to stress the pipeline.
 */
public class MultiSymbolLoadGen {
    private final int symbols;
    private final int perSecondPerSymbol;
    private final Consumer<QuoteUpdate> sink;
    private final String[] symbolsArr;
    private final Random rnd = new Random(42);

    public MultiSymbolLoadGen(int symbols, int perSecondPerSymbol, Consumer<QuoteUpdate> sink) {
        this.symbols = symbols;
        this.perSecondPerSymbol = perSecondPerSymbol;
        this.sink = sink;
        this.symbolsArr = new String[symbols];
        for (int i = 0; i < symbols; i++) symbolsArr[i] = "XS" + String.format("%010d", i);
    }

    public void runForMillis(long millis) throws InterruptedException {
        long start = System.nanoTime();
        long end = start + millis * 1_000_000L;
        long now;
        long seq = 0;
        while ((now = System.nanoTime()) < end) {
            for (int i = 0; i < symbols; i++) {
                String sym = symbolsArr[i];
                long ts = System.nanoTime();
                long bid = 500 + rnd.nextInt(10_000);
                long ask = 500 + rnd.nextInt(10_000);
                if ((seq & 0x3FF) == 0) bid *= 10;
                if ((seq & 0x7FF) == 0) ask *= 12;

                sink.accept(new QuoteUpdate(sym, Side.BID, bid, ts));
                sink.accept(new QuoteUpdate(sym, Side.ASK, ask, ts));
            }
            seq++;
            Thread.yield();
        }
    }
}
