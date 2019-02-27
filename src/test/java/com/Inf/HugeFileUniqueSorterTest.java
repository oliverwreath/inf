package com.Inf;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.Inf.HugeFileUniqueSorter.sortWords;
import static com.Inf.HugeFileUniqueSorter.trimNonAlphaDigit;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Author: Oliver
 */
class HugeFileUniqueSorterTest {
    private static final Logger logger = LoggerFactory.getLogger(HugeFileUniqueSorterTest.class);

    private static final String TEST_INPUT_STRING = "\"mellifluous\",";
    private static final String EXPECT_OUTPUT_STRING = "mellifluous";

    @BeforeAll
    static void setUp() {
        logger.info("Test Begin!");
    }

    @Test
    void testTrimNonAlphaDigit() {
        assertEquals(EXPECT_OUTPUT_STRING, trimNonAlphaDigit(TEST_INPUT_STRING));
    }

    @Test
    void testSortWords() {
        HashSet<String> hashSet = new HashSet<>(Arrays.asList("long", "time", "ago", "info", "big", "data"));
        List<String> EXPECTED_LST = new ArrayList<>(Arrays.asList("ago", "big", "data", "info", "long", "time"));
        assertEquals(EXPECTED_LST, sortWords(hashSet));
    }

    @AfterAll
    static void cleanUp() {
        logger.info("Test End!");
    }
}
