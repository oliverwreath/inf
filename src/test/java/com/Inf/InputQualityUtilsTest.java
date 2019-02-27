package com.Inf;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Author: Oliver
 */
public class InputQualityUtilsTest {
    private static final Logger logger = LoggerFactory.getLogger(InputQualityUtilsTest.class);

    private static final String TEST_INPUT_STRING = "\"mellifluous\",";
    private static final String EXPECT_OUTPUT_STRING = "mellifluous";

    @BeforeAll
    static void setUp() {
        logger.info("Test Begin!");
    }

    @Test
    void testInputQualityUtils() {
        assertTrue(InputQualityUtils.isEmpty((String) null));
        assertTrue(InputQualityUtils.isEmpty(""));
        assertFalse(InputQualityUtils.isEmpty(TEST_INPUT_STRING));

        assertTrue(InputQualityUtils.isEmpty((Collection<?>) null));
        assertTrue(InputQualityUtils.isEmpty(new HashSet<>()));
        assertFalse(InputQualityUtils.isEmpty(new HashSet<>(Arrays.asList("123", "sdf"))));
    }

    @AfterAll
    static void cleanUp() {
        logger.info("Test End!\n");
    }
}
