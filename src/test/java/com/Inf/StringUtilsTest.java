package com.Inf;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.Inf.StringUtils.trimNonAlphaDigit;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Author: Oliver
 */
public class StringUtilsTest {
    private static final Logger logger = LoggerFactory.getLogger(StringUtilsTest.class);

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

    @AfterAll
    static void cleanUp() {
        logger.info("Test End!\n");
    }
}
