package com.yj.redis.monitor.analyzer.increment;

import java.util.regex.Pattern;

public enum SegmentType {

    NUMBER(Pattern.compile("^\\d+$")),
    UUID(Pattern.compile("^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$")),
    DATE(Pattern.compile("^\\d{4}[-/]?\\d{2}[-/]?\\d{2}$")),
    HEX(Pattern.compile("^[0-9a-fA-F]{16,}$")),
    STRING(null),
    FIXED(null);

    private final Pattern pattern;

    SegmentType(Pattern pattern) {
        this.pattern = pattern;
    }

    /**
     * Detection order: UUID, DATE, NUMBER, HEX, defaulting to FIXED.
     * DATE is checked before NUMBER so that strings like "20240115"
     * are correctly identified as dates rather than plain numbers.
     */
    public static SegmentType detect(String segment) {
        if (UUID.matches(segment)) return UUID;
        if (DATE.matches(segment)) return DATE;
        if (NUMBER.matches(segment)) return NUMBER;
        if (HEX.matches(segment)) return HEX;
        return FIXED;
    }

    /**
     * Checks if the given segment matches this type's regex pattern.
     * STRING and FIXED match any segment.
     */
    public boolean matches(String segment) {
        if (pattern == null) {
            return true;
        }
        return pattern.matcher(segment).matches();
    }

    /**
     * Returns true if this type represents a variable pattern (not FIXED).
     */
    public boolean isVariable() {
        return this != FIXED;
    }
}
