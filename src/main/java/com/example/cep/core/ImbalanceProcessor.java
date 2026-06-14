package com.example.cep.core;

import com.example.cep.model.QuoteUpdate;
import com.example.cep.model.Side;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * CEP processor: detects order book imbalance.
 * Imbalance = (bid - ask) / (bid + ask)
 */
public class ImbalanceProcessor {

    public interface AlertSink {
        void onImbalanceAlert(String symbol, double imbalance, long bidSize, long askSize, long tsNanos);
        default void onImbalanceCleared(String symbol, long tsNanos) {}
    }

    private static final class BookSideState {
        long size;
        long tsNanos;
    }

    private static final class OrderBookState {
        final BookSideState bid = new BookSideState();
        final BookSideState ask = new BookSideState();
        boolean inAlert = false;
        long crossedSinceNanos = -1;
    }

    private final ConcurrentMap<String, OrderBookState> books = new ConcurrentHashMap<>();
    private final double threshold;
    private final double hysteresis;
    private final long minTotalSize;
    private final long sustainNanos;
    private final AlertSink sink;

    public ImbalanceProcessor(double threshold, double hysteresis, long minTotalSize, long sustainMillis, AlertSink sink) {
        if (threshold <= 0 || threshold >= 1) throw new IllegalArgumentException("threshold in (0,1)");
        if (hysteresis <= 0 || hysteresis >= threshold) throw new IllegalArgumentException("hysteresis in (0, threshold)");
        this.threshold = threshold;
        this.hysteresis = hysteresis;
        this.minTotalSize = Math.max(0, minTotalSize);
        this.sustainNanos = Math.max(0, sustainMillis) * 1_000_000L;
        this.sink = Objects.requireNonNull(sink);
    }

    public void onQuote(QuoteUpdate q) {
        final OrderBookState ob = books.computeIfAbsent(q.symbol(), k -> new OrderBookState());
        final BookSideState side = (q.side() == Side.BID) ? ob.bid : ob.ask;

        // Out-of-order protection
        if (q.tsNanos() <= side.tsNanos) return;
        side.tsNanos = q.tsNanos;
        side.size = Math.max(0, q.size());

        final long latestTs = Math.max(ob.bid.tsNanos, ob.ask.tsNanos);
        final long bid = ob.bid.size;
        final long ask = ob.ask.size;
        final long total = bid + ask;

        if (total < minTotalSize || total == 0) {
            if (ob.inAlert) {
                ob.inAlert = false;
                ob.crossedSinceNanos = -1;
                sink.onImbalanceCleared(q.symbol(), latestTs);
            }
            return;
        }

        final double imbalance = (double)(bid - ask) / (double) total;
        final double absImb = Math.abs(imbalance);

        if (!ob.inAlert) {
            if (absImb >= threshold) {
                if (sustainNanos == 0) {
                    ob.inAlert = true;
                    ob.crossedSinceNanos = latestTs;
                    sink.onImbalanceAlert(q.symbol(), imbalance, bid, ask, latestTs);
                } else {
                    if (ob.crossedSinceNanos < 0) ob.crossedSinceNanos = latestTs;
                    else if (latestTs - ob.crossedSinceNanos >= sustainNanos) {
                        ob.inAlert = true;
                        sink.onImbalanceAlert(q.symbol(), imbalance, bid, ask, latestTs);
                    }
                }
            } else {
                ob.crossedSinceNanos = -1;
            }
        } else {
            if (absImb < hysteresis) {
                ob.inAlert = false;
                ob.crossedSinceNanos = -1;
                sink.onImbalanceCleared(q.symbol(), latestTs);
            }
        }
    }

    public Optional<Double> getImbalance(String symbol) {
        OrderBookState ob = books.get(symbol);
        if (ob == null) return Optional.empty();
        long total = ob.bid.size + ob.ask.size;
        if (total == 0) return Optional.empty();
        return Optional.of(((double)(ob.bid.size - ob.ask.size)) / (double) total);
    }
}
