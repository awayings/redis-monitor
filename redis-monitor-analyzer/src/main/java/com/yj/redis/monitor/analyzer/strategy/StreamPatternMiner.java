package com.yj.redis.monitor.analyzer.strategy;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 流式字符串 *-Pattern 聚合器
 * 核心特性：
 * 1. 增量模式合并：常量保留，差异位置泛化为 *
 * 2. 前缀索引：O(1) 定位候选模式簇
 * 3. 时间衰减：防止内存无限增长
 * 4. 区分度排序：特异性 × 支撑度 × 泛化惩罚
 */
public class StreamPatternMiner {

    // ==================== 配置参数 ====================
    private final int minPatternCount;          // 输出阈值 θ
    private final double mergeThreshold;        // 合并相似度 δ (0~1)
    private final int maxPatterns;              // 内存上限
    private final double decayFactor;           // 衰减系数 (0~1)，如 0.95
    private final long decayIntervalMs;         // 衰减周期
    private final int topK;                     // 输出 Top-K
    private final Tokenizer tokenizer;          // 分词器

    // ==================== 状态存储 ====================
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final Map<String, List<PatternNode>> prefixIndex = new ConcurrentHashMap<>();
    private final List<PatternNode> allPatterns = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService decayExecutor = Executors.newSingleThreadScheduledExecutor();

    // ==================== 构造函数 ====================
    public StreamPatternMiner(Builder builder) {
        this.minPatternCount = builder.minPatternCount;
        this.mergeThreshold = builder.mergeThreshold;
        this.maxPatterns = builder.maxPatterns;
        this.decayFactor = builder.decayFactor;
        this.decayIntervalMs = builder.decayIntervalMs;
        this.topK = builder.topK;
        this.tokenizer = builder.tokenizer;

        // 启动定时衰减任务
        this.decayExecutor.scheduleAtFixedRate(
            this::decayAll, 
            decayIntervalMs, 
            decayIntervalMs, 
            TimeUnit.MILLISECONDS
        );
    }

    // ==================== 核心数据结构 ====================
    public static class PatternNode {
        private final List<String> tokens;      // 模式骨架，"*" 表示通配
        private volatile double count;          // 带衰减的计数（用 double 支持衰减）
        private volatile long lastSeen;         // 最后命中时间戳
        private final int constPosCount;        // 常量位置数（缓存，避免重复计算）

        public PatternNode(List<String> tokens, double count, long lastSeen) {
            this.tokens = new ArrayList<>(tokens);
            this.count = count;
            this.lastSeen = lastSeen;
            this.constPosCount = (int) tokens.stream().filter(t -> !"*".equals(t)).count();
        }

        public List<String> getTokens() { return Collections.unmodifiableList(tokens); }
        public double getCount() { return count; }
        public long getLastSeen() { return lastSeen; }
        public int getConstPosCount() { return constPosCount; }
        public int length() { return tokens.size(); }

        public boolean isConstAt(int idx) {
            return idx < tokens.size() && !"*".equals(tokens.get(idx));
        }

        public String getTokenAt(int idx) {
            return idx < tokens.size() ? tokens.get(idx) : null;
        }

        public void merge(List<String> incomingTokens) {
            // 位置级合并：不同则置 *
            int maxLen = Math.max(this.tokens.size(), incomingTokens.size());
            List<String> merged = new ArrayList<>(maxLen);
            for (int i = 0; i < maxLen; i++) {
                String a = i < this.tokens.size() ? this.tokens.get(i) : null;
                String b = i < incomingTokens.size() ? incomingTokens.get(i) : null;
                if (a != null && a.equals(b)) {
                    merged.add(a);
                } else {
                    merged.add("*");
                }
            }
            this.tokens.clear();
            this.tokens.addAll(merged);
        }

        public void increment(long now) {
            this.count += 1.0;
            this.lastSeen = now;
        }

        public void applyDecay(double factor) {
            this.count *= factor;
        }

        @Override
        public String toString() {
            return String.join(" ", tokens) + " | count=" + String.format("%.1f", count);
        }
    }

    // ==================== 分词器接口 ====================
    @FunctionalInterface
    public interface Tokenizer {
        List<String> tokenize(String input);
    }

    // 默认分词器：空格/制表符拆分，纯数字自动泛化为 *
    public static Tokenizer defaultTokenizer() {
        return input -> {
            if (input == null || input.isEmpty()) return Collections.emptyList();
            String[] parts = input.split("\\s+");
            List<String> tokens = new ArrayList<>();
            for (String p : parts) {
                if (p.matches("\\d+")) {
                    tokens.add("*");  // 纯数字自动泛化，减少模式碎片化
                } else {
                    tokens.add(p);
                }
            }
            return tokens;
        };
    }

    public static Tokenizer smartTokenizer() {
        return input -> {
            if (input == null || input.isEmpty()) return Collections.emptyList();

            String[] spaceParts = input.split("\\s+");
            List<String> tokens = new ArrayList<>();

            for (String part : spaceParts) {
                // 按下划线/连字符/点号二次拆分
                String[] subParts = part.split("[_\\-.]");
                for (String sub : subParts) {
                    if (sub.isEmpty()) continue;
                    tokens.add(normalizeToken(sub));
                }
            }
            return tokens;
        };
    }

    /**
     * 核心泛化逻辑：识别无业务语义的随机 ID
     */
    private static String normalizeToken(String token) {
        // 纯数字直接泛化
        if (token.matches("\\d+")) return "*";

        // 随机 ID 特征：长度>8 且同时包含字母和数字（如 V16df7aa6324e, a1b2c3d4）
        // 可根据业务调整阈值，如 >6 或 >10
        if (token.length() > 8 && hasMixedAlphaDigit(token)) {
            return "*";
        }

        // 短 token 或纯字母保留原样（SKR, LOGIN, TOKEN, GOODS...）
        return token;
    }

    private static boolean hasMixedAlphaDigit(String s) {
        boolean hasDigit = false, hasLetter = false;
        for (char c : s.toCharArray()) {
            if (Character.isDigit(c)) hasDigit = true;
            if (Character.isLetter(c)) hasLetter = true;
            if (hasDigit && hasLetter) return true;
        }
        return false;
    }


    // ==================== 流式处理入口 ====================
    public void process(String rawString) {
        if (rawString == null || rawString.isEmpty()) return;
        
        totalProcessed.incrementAndGet();
        List<String> tokens = tokenizer.tokenize(rawString);
        if (tokens.isEmpty()) return;

        long now = System.currentTimeMillis();
        String firstToken = tokens.get(0);

        // 1. 通过前缀索引获取候选模式（O(1) 定位）
        List<PatternNode> candidates = prefixIndex.getOrDefault(firstToken, Collections.emptyList());
        
        // 2. 找最佳匹配
        PatternNode bestMatch = null;
        double bestSim = 0.0;

        for (PatternNode candidate : candidates) {
            double sim = similarity(candidate, tokens);
            if (sim >= mergeThreshold && sim > bestSim) {
                bestSim = sim;
                bestMatch = candidate;
            }
        }

        // 3. 合并或新建
        if (bestMatch != null) {
            synchronized (bestMatch) {
                // 双重检查：可能并发下已被其他线程修改
                if (similarity(bestMatch, tokens) >= mergeThreshold) {
                    bestMatch.merge(tokens);
                    bestMatch.increment(now);
                } else {
                    createNewPattern(tokens, now);
                }
            }
        } else {
            createNewPattern(tokens, now);
        }

        // 4. 内存保护：超限时剪枝
        if (allPatterns.size() > maxPatterns * 1.2) {
            prune();
        }
    }

    // ==================== 相似度计算 ====================
    private double similarity(PatternNode pattern, List<String> tokens) {
        int maxLen = Math.max(pattern.length(), tokens.size());
        if (maxLen == 0) return 1.0;

        int matchCount = 0;
        int minLen = Math.min(pattern.length(), tokens.size());
        
        for (int i = 0; i < minLen; i++) {
            String pToken = pattern.getTokenAt(i);
            if ("*".equals(pToken) || pToken.equals(tokens.get(i))) {
                matchCount++;
            }
        }
        // 长度差异惩罚
        return (double) matchCount / maxLen;
    }

    // ==================== 新建模式 ====================
    private void createNewPattern(List<String> tokens, long now) {
        PatternNode newNode = new PatternNode(tokens, 1.0, now);
        allPatterns.add(newNode);
        
        // 建立前缀索引（首 token 或首 token 为 * 时加入通配索引）
        String first = tokens.get(0);
        prefixIndex.computeIfAbsent(first, k -> new CopyOnWriteArrayList<>()).add(newNode);
        
        // 若首 token 不是 *，同时加入 "*" 通配索引，支持后续以 * 开头的模式匹配
        if (!"*".equals(first)) {
            prefixIndex.computeIfAbsent("*", k -> new CopyOnWriteArrayList<>()).add(newNode);
        }
    }

    // ==================== 衰减与剪枝 ====================
    private void decayAll() {
        long now = System.currentTimeMillis();
        for (PatternNode p : allPatterns) {
            // 时间衰减：越久未命中衰减越重
            long idleMs = now - p.getLastSeen();
            double idleFactor = Math.pow(decayFactor, idleMs / decayIntervalMs);
            p.applyDecay(idleFactor);
        }
    }

    private void prune() {
        // 按衰减后计数排序，淘汰尾部 20%
        List<PatternNode> sorted = new ArrayList<>(allPatterns);
        sorted.sort(Comparator.comparingDouble(PatternNode::getCount));
        
        int removeCount = (int) (sorted.size() * 0.2);
        List<PatternNode> toRemove = sorted.subList(0, removeCount);
        
        for (PatternNode p : toRemove) {
            allPatterns.remove(p);
            // 从索引中清理
            String first = p.getTokenAt(0);
            if (first != null) {
                List<PatternNode> list = prefixIndex.get(first);
                if (list != null) list.remove(p);
            }
        }
    }

    // ==================== 区分度计算 & Top-K 提取 ====================
    public List<ScoredPattern> extractPatterns() {
        long N = totalProcessed.get();
        List<ScoredPattern> scored = new ArrayList<>();

        for (PatternNode p : allPatterns) {
            if (p.getCount() >= minPatternCount) {
                double score = discriminationScore(p, N);
                scored.add(new ScoredPattern(p, score));
            }
        }

        // 按区分度降序，取 Top-K
        scored.sort((a, b) -> Double.compare(b.score, a.score));
        return scored.size() > topK ? scored.subList(0, topK) : scored;
    }

    /**
     * 区分度公式：
     * D(p) = (常量比例) × ln(1+count) × (1 - count/N)^α
     * 其中 α 控制对过度泛化的惩罚强度
     */
    private double discriminationScore(PatternNode p, long total) {
        double specificity = (double) p.getConstPosCount() / p.length();
        double support = Math.log(1 + p.getCount());
        double overGeneralizationPenalty = Math.pow(1 - Math.min(p.getCount() / total, 1.0), 0.5);
        
        return specificity * support * overGeneralizationPenalty;
    }

    public static class ScoredPattern {
        public final PatternNode pattern;
        public final double score;

        public ScoredPattern(PatternNode pattern, double score) {
            this.pattern = pattern;
            this.score = score;
        }

        @Override
        public String toString() {
            return String.format("[score=%.3f] %s", score, pattern.toString());
        }
    }

    // ==================== Builder ====================
    public static class Builder {
        private int minPatternCount = 50;
        private double mergeThreshold = 0.6;
        private int maxPatterns = 10000;
        private double decayFactor = 0.95;
        private long decayIntervalMs = 60000; // 1分钟
        private int topK = 100;
        private Tokenizer tokenizer = smartTokenizer();

        public Builder minPatternCount(int val) { this.minPatternCount = val; return this; }
        public Builder mergeThreshold(double val) { this.mergeThreshold = val; return this; }
        public Builder maxPatterns(int val) { this.maxPatterns = val; return this; }
        public Builder decayFactor(double val) { this.decayFactor = val; return this; }
        public Builder decayIntervalMs(long val) { this.decayIntervalMs = val; return this; }
        public Builder topK(int val) { this.topK = val; return this; }
        public Builder tokenizer(Tokenizer val) { this.tokenizer = val; return this; }

        public StreamPatternMiner build() {
            return new StreamPatternMiner(this);
        }
    }

    // ==================== 关闭资源 ====================
    public void shutdown() {
        decayExecutor.shutdown();
    }

    // ==================== 使用示例 ====================
    public static void main(String[] args) {
        StreamPatternMiner miner = new StreamPatternMiner.Builder()
            .minPatternCount(3)          // 阈值低方便演示
            .mergeThreshold(0.6)
            .maxPatterns(1000)
            .decayFactor(0.95)
            .topK(10)
            .build();

        // 模拟流式输入
        String[] logs = {
            "User 123 login from 192.168.1.1 success",
            "User 456 login from 10.0.0.1 success",
            "User 789 login from 172.16.0.1 failed",
            "Order 1001 created by user_A amount 500",
            "Order 1002 created by user_B amount 800",
            "Order 1003 created by user_C amount 1200",
            "ERROR code 500 at module payment",
            "ERROR code 404 at module user",
            "ERROR code 503 at module payment",
            "Random noise message here",
                "SKR_LOGIN_TOKEN_V16df7aa6324e",
                "SKR_LOGIN_TOKEN_V107e6d1a20a0",
                "SKR_LOGIN_TOKEN_V18c9e21099eb",
                "SYNC_CUSTOM_TOP_GOODS_16880",
                "SYNC_CUSTOM_TOP_GOODS_16881",
                "SYNC_CUSTOM_TOP_GOODS_16885",
        };

        for (String log : logs) {
            miner.process(log);
        }

        // 提取高频区分模式
        List<ScoredPattern> patterns = miner.extractPatterns();
        System.out.println("=== Top Patterns ===");
        patterns.forEach(System.out::println);

        miner.shutdown();
    }
}