package com.Inf;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static com.Inf.HugeFileUniqueSorter.sortWords;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Author: Oliver
 */
class HugeFileUniqueSorterTest {
    private static final Logger logger = LoggerFactory.getLogger(HugeFileUniqueSorterTest.class);

    @BeforeAll
    static void setUp() {
        logger.info("Test Begin!");
    }

    @Test
    void testSortWords() {
        final HashSet<String> INPUT_SET = new HashSet<>(Arrays.asList("long", "time", "ago", "info", "big", "data"));
        final List<String> EXPECTED_LST = new ArrayList<>(Arrays.asList("ago", "big", "data", "info", "long", "time"));
        assertEquals(EXPECTED_LST, sortWords(INPUT_SET));
    }

    @AfterAll
    static void cleanUp() {
        logger.info("Test End!\n");
    }
}
