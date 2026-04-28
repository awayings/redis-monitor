package com.yj.redis.monitor.analyzer.increment;

import com.yj.redis.monitor.core.RedisConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MemoryIncrementAnalyzerIntegrationTest {

    private static final String HOST = "10.43.28.185";
    private static final int PORT = 6379;

    private Jedis jedis;

    @Before
    public void setUp() {
        jedis = new RedisConnectionFactory(HOST, PORT).createConnection();
    }

    @After
    public void tearDown() {
        if (jedis != null) {
            jedis.close();
        }
    }

    @Test
    public void testEndToEndWithKnownKeys() throws Exception {
        // Write keys with known patterns (TTL ensures self-cleanup)
        for (int i = 0; i < 50; i++) {
            jedis.setex("__test_user:" + i + ":profile", 60, "data");
        }
        for (int i = 0; i < 30; i++) {
            jedis.setex("__test_order:" + i + ":detail", 60, "order_data");
        }

        // Run analyzer for short duration
        Args args = Args.parse(new String[]{
                "--host=" + HOST, "--port=" + PORT, "--duration=5",
                "--samples-per-pattern=5", "--ttl-samples-per-pattern=3",
                "--upgrade-threshold=3", "--top-n=10"
        });
        MemoryIncrementAnalyzer analyzer = new MemoryIncrementAnalyzer(args);

        // Simulate some traffic during monitoring
        Thread writer = new Thread(() -> {
            Jedis w = new RedisConnectionFactory(HOST, PORT).createConnection();
            for (int i = 0; i < 20; i++) {
                w.setex("__test_user:" + i + ":profile", 60, "updated");
                try {
                    Thread.sleep(100);
                } catch (Exception ignored) {
                }
            }
            w.close();
        });
        writer.start();

        analyzer.run();
        writer.join();

        // Verify aggregator has data
        assertTrue(analyzer.getAggregator().getTotalWriteCount() > 0);
    }

    @Test
    public void testNoKeysCapturedProducesEmptyReport() {
        Args args = Args.parse(new String[]{
                "--host=" + HOST, "--port=" + PORT, "--duration=1", "--top-n=10"
        });
        MemoryIncrementAnalyzer analyzer = new MemoryIncrementAnalyzer(args);
        analyzer.run();
        assertEquals(0, analyzer.getAggregator().getTotalWriteCount());
    }

    @Test
    public void testJsonOutputFlag() {
        Args args = Args.parse(new String[]{
                "--host=" + HOST, "--port=" + PORT, "--duration=1",
                "--output=json", "--top-n=5"
        });
        assertEquals("json", args.getOutput());
    }
}
