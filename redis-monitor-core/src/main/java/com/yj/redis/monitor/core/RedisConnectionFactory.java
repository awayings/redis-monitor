package com.yj.redis.monitor.core;

import redis.clients.jedis.Jedis;

public class RedisConnectionFactory {

    private final String host;
    private final int port;
    private final int connectionTimeoutMs;
    private final int socketTimeoutMs;
    private final String password;

    public RedisConnectionFactory(String host, int port) {
        this(host, port, 2000, 5000, null);
    }

    public RedisConnectionFactory(String host, int port, int connectionTimeoutMs,
                                   int socketTimeoutMs, String password) {
        this.host = host;
        this.port = port;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.socketTimeoutMs = socketTimeoutMs;
        this.password = password;
    }

    public Jedis createConnection() {
        Jedis jedis = new Jedis(host, port, connectionTimeoutMs, socketTimeoutMs);
        if (password != null && !password.isEmpty()) {
            jedis.auth(password);
        }
        return jedis;
    }
}
