package com.yj.redis.monitor.analyzer.increment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PrefixTrie {

    public static final int MAX_KEY_LENGTH = 200;
    public static final int MAX_UNIQUE_KEYS = 100000;
    public static final int MAX_NODES = 500000;

    static class TrieNode {
        Map<Character, TrieNode> children = new HashMap<>();
        int count = 0;
    }

    private final TrieNode root = new TrieNode();
    private final int upgradeThreshold;
    private final Set<String> seenKeys = new HashSet<>();
    private int totalNodes = 1; // root counts as 1

    public PrefixTrie(int upgradeThreshold) {
        this.upgradeThreshold = upgradeThreshold;
    }

    /**
     * Inserts a key into the trie.
     *
     * @return true if the key was accepted (inserted or was already present)
     */
    public boolean insert(String key) {
        if (key == null || key.length() > MAX_KEY_LENGTH) {
            return false;
        }

        // If already seen, just increment counts along existing path
        if (seenKeys.contains(key)) {
            TrieNode node = root;
            for (int i = 0; i < key.length(); i++) {
                node = node.children.get(key.charAt(i));
                node.count++;
            }
            return true;
        }

        // Enforce max unique keys guard
        if (seenKeys.size() >= MAX_UNIQUE_KEYS) {
            return false;
        }

        // Traverse and create nodes as needed
        TrieNode node = root;
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            TrieNode child = node.children.get(c);
            if (child == null) {
                if (totalNodes >= MAX_NODES) {
                    return false;
                }
                child = new TrieNode();
                node.children.put(c, child);
                totalNodes++;
            }
            child.count++;
            node = child;
        }

        seenKeys.add(key);
        return true;
    }

    public int getUniqueKeyCount() {
        return seenKeys.size();
    }

    /**
     * Extracts patterns from the trie via DFS.
     * Patterns are emitted for subtrees that meet the upgrade threshold.
     *
     * @return map of pattern to key count
     */
    public Map<String, Integer> extractPatterns() {
        Map<String, Integer> patterns = new HashMap<>();
        int totalUncovered = 0;

        for (Map.Entry<Character, TrieNode> entry : root.children.entrySet()) {
            totalUncovered += dfs(entry.getValue(), String.valueOf(entry.getKey()), patterns);
        }

        if (totalUncovered > 0) {
            patterns.put("*", totalUncovered);
        }

        return patterns;
    }

    /**
     * DFS traversal of the trie.
     *
     * @param node     current trie node
     * @param prefix   accumulated prefix leading to this node
     * @param patterns output map of patterns
     * @return number of uncovered keys in this subtree
     */
    private int dfs(TrieNode node, String prefix, Map<String, Integer> patterns) {
        if (node.count < upgradeThreshold) {
            return node.count;
        }

        // Count children whose count is at or above the upgrade threshold
        List<Map.Entry<Character, TrieNode>> aboveThreshold = new ArrayList<>();
        for (Map.Entry<Character, TrieNode> entry : node.children.entrySet()) {
            if (entry.getValue().count >= upgradeThreshold) {
                aboveThreshold.add(entry);
            }
        }

        int childrenAbove = aboveThreshold.size();

        if (childrenAbove >= 2 || node.children.isEmpty()) {
            patterns.put(prefix + "*", node.count);
            return 0;
        }

        if (childrenAbove == 0) {
            patterns.put(prefix + "*", node.count);
            return 0;
        }

        // Exactly one child above threshold — recurse into it
        int uncovered = 0;
        for (Map.Entry<Character, TrieNode> entry : node.children.entrySet()) {
            TrieNode child = entry.getValue();
            if (child.count < upgradeThreshold) {
                uncovered += child.count;
            }
        }

        Map.Entry<Character, TrieNode> onlyChild = aboveThreshold.get(0);
        uncovered += dfs(onlyChild.getValue(), prefix + onlyChild.getKey(), patterns);

        return uncovered;
    }
}
