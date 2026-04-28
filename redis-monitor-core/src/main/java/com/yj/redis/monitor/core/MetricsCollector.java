package com.yj.redis.monitor.core;

import java.util.Map;

public interface MetricsCollector {

    Map<String, String> collectInfo();

    Map<String, String> collectStats();
}
