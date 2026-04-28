package com.yj.redis.monitor.analyzer.increment;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PatternClusterState {

    private String currentPattern;
    private int keyCount;
    private final List<SegmentType> segmentTypes;
    private final List<String> fixedValues;
    private final List<Set<String>> distinctValues;

    public PatternClusterState(String pattern, List<SegmentType> segmentTypes, List<String> fixedValues) {
        this.currentPattern = pattern;
        this.keyCount = 0;
        this.segmentTypes = segmentTypes;
        this.fixedValues = fixedValues;
        this.distinctValues = new ArrayList<>(segmentTypes.size());
        for (int i = 0; i < segmentTypes.size(); i++) {
            Set<String> set = new HashSet<>();
            if (segmentTypes.get(i) == SegmentType.FIXED) {
                set.add(fixedValues.get(i));
            }
            distinctValues.add(set);
        }
    }

    public String getCurrentPattern() {
        return currentPattern;
    }

    public int getKeyCount() {
        return keyCount;
    }

    public int getSegmentCount() {
        return segmentTypes.size();
    }

    public SegmentType getSegmentType(int index) {
        return segmentTypes.get(index);
    }

    public String getFixedValue(int index) {
        return fixedValues.get(index);
    }

    public int getDistinctCount(int index) {
        return distinctValues.get(index).size();
    }

    public void incrementCount() {
        keyCount++;
    }

    public void addKeyCount(int n) {
        keyCount += n;
    }

    /**
     * Records a distinct value for the segment at the given index.
     * If the segment is FIXED and the distinct set size reaches upgradeThreshold,
     * upgrades it to STRING, clears the fixed value, rebuilds the pattern,
     * and returns true. Otherwise returns false.
     *
     * @param index            segment index
     * @param value            distinct value to record
     * @param upgradeThreshold when distinct set size reaches this threshold, upgrade FIXED to STRING
     * @return true if the segment was upgraded from FIXED to STRING
     */
    public boolean recordDistinctValue(int index, String value, int upgradeThreshold) {
        Set<String> set = distinctValues.get(index);
        set.add(value);

        if (segmentTypes.get(index) == SegmentType.FIXED && set.size() >= upgradeThreshold) {
            segmentTypes.set(index, SegmentType.STRING);
            fixedValues.set(index, null);
            rebuildPattern();
            return true;
        }
        return false;
    }

    /**
     * Rebuilds currentPattern from segmentTypes and fixedValues.
     * FIXED segments use their fixed value, all others use "*".
     */
    private void rebuildPattern() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < segmentTypes.size(); i++) {
            if (i > 0) {
                sb.append(":");
            }
            if (segmentTypes.get(i) == SegmentType.FIXED) {
                sb.append(fixedValues.get(i));
            } else {
                sb.append("*");
            }
        }
        this.currentPattern = sb.toString();
    }
}
