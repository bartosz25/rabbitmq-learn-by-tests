package com.waitingforcode.util;

public enum  ExchangeTypes {

    DIRECT("direct"),
    FANOUT("fanout"),
    TOPIC("topic"),
    HEADERS("headers");

    private final String name;

    private ExchangeTypes(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
