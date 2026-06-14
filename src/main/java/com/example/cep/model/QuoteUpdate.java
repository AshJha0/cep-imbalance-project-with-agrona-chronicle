package com.example.cep.model;

public record QuoteUpdate(String symbol, Side side, long size, long tsNanos) {}
