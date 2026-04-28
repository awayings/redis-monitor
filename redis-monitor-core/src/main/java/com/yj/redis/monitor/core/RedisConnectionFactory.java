package com.yj.redis.monitor.core;

import redis.clients.jedis.Jedis;

public class RedisConnectionFactory {

    private final String host;
    private final int port;

    public RedisConnectionFactory(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Jedis createConnection() {
        return new Jedis(host, port);
    }
}
