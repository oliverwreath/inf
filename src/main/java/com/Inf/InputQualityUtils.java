package com.Inf;

import java.util.Collection;

/**
 * Author: Oliver
 */
class InputQualityUtils {
    static boolean isEmpty(final String inputString) {
        return inputString == null || inputString.isEmpty();
    }

    static boolean isEmpty(final Collection<?> inputCollections) {
        return inputCollections == null || inputCollections.isEmpty();
    }
}
