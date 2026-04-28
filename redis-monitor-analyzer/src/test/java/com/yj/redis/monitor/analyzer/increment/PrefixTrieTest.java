package com.yj.redis.monitor.analyzer.increment;

import org.junit.Test;
import static org.junit.Assert.*;

public class PrefixTrieTest {

    @Test
    public void testInsertAndExtractSinglePath() {
        PrefixTrie trie = new PrefixTrie(2);
        assertTrue(trie.insert("key:1"));
        assertTrue(trie.insert("key:2"));
        assertTrue(trie.insert("key:3"));

        assertFalse(trie.extractPatterns().isEmpty());
    }

    @Test
    public void testInsertRejectsKeyTooLong() {
        PrefixTrie trie = new PrefixTrie(2);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 201; i++) {
            sb.append('a');
        }
        assertFalse(trie.insert(sb.toString()));
    }

    @Test
    public void testInsertDuplicateKeys() {
        PrefixTrie trie = new PrefixTrie(2);
        assertTrue(trie.insert("hello"));
        assertTrue(trie.insert("hello"));
        assertEquals(1, trie.getUniqueKeyCount());
    }

    @Test
    public void testExtractBranchPattern() {
        PrefixTrie trie = new PrefixTrie(3);
        trie.insert("user_profile_1001");
        trie.insert("user_profile_1002");
        trie.insert("user_profile_2001");

        assertTrue(trie.extractPatterns().containsKey("user_profile_*"));
    }

    @Test
    public void testExtractWithHighThreshold() {
        PrefixTrie trie = new PrefixTrie(100);
        trie.insert("foo");
        trie.insert("bar");
        trie.insert("baz");

        assertTrue(trie.extractPatterns().containsKey("*"));
    }

    @Test
    public void testInsertUpToMaxUniqueKeys() {
        PrefixTrie trie = new PrefixTrie(5);
        assertTrue(trie.insert("alpha"));
        assertTrue(trie.insert("beta"));
        assertTrue(trie.insert("gamma"));
    }
}
