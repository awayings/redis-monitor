package com.yj.redis.monitor.analyzer.increment;

import com.yj.redis.monitor.core.RedisConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore("Requires running Redis on localhost:6379")
public class MemoryIncrementAnalyzerIntegrationTest {

    private Jedis jedis;

    @Before
    public void setUp() {
        jedis = new RedisConnectionFactory("localhost", 6379).createConnection();
        jedis.flushAll();
    }

    @After
    public void tearDown() {
        if (jedis != null) {
            jedis.flushAll();
            jedis.close();
        }
    }

    @Test
    public void testEndToEndWithKnownKeys() throws Exception {
        // Write keys with known patterns
        for (int i = 0; i < 50; i++) {
            jedis.setex("user:" + i + ":profile", 3600, "data");
        }
        for (int i = 0; i < 30; i++) {
            jedis.setex("order:" + i + ":detail", 1800, "order_data");
        }

        // Run analyzer for short duration
        Args args = Args.parse(new String[]{
                "--host=localhost", "--port=6379", "--duration=5",
                "--samples-per-pattern=5", "--ttl-samples-per-pattern=3",
                "--upgrade-threshold=3", "--top-n=10"
        });
        MemoryIncrementAnalyzer analyzer = new MemoryIncrementAnalyzer(args);

        // Simulate some traffic during monitoring
        Thread writer = new Thread(() -> {
            Jedis w = new RedisConnectionFactory("localhost", 6379).createConnection();
            for (int i = 0; i < 20; i++) {
                w.setex("user:" + i + ":profile", 3600, "updated");
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
                "--host=localhost", "--port=6379", "--duration=1", "--top-n=10"
        });
        MemoryIncrementAnalyzer analyzer = new MemoryIncrementAnalyzer(args);
        analyzer.run();
        assertEquals(0, analyzer.getAggregator().getTotalWriteCount());
    }

    @Test
    public void testJsonOutputFlag() {
        Args args = Args.parse(new String[]{
                "--host=localhost", "--port=6379", "--duration=1",
                "--output=json", "--top-n=5"
        });
        assertEquals("json", args.getOutput());
    }
}
