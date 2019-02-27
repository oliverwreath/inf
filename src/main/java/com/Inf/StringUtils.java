package com.Inf;

import org.apache.commons.lang3.Validate;

/**
 * Author: Oliver
 */
public class StringUtils {
    public static String trimNonAlphaDigit(final String inputString) {
        // Validate Preconditions
        Validate.notBlank(inputString);

        return inputString.replaceAll("[^\\p{IsAlphabetic}^\\p{IsDigit}]", "");
    }
}
