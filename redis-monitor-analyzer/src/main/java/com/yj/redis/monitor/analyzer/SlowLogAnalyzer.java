package com.yj.redis.monitor.analyzer;

import java.util.List;

public interface SlowLogAnalyzer {

    List<SlowLogEntry> analyzeSlowLogs(int count);
}
