package com.Inf;

import org.apache.commons.lang3.Validate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

import static com.Inf.HugeFileUniqueSorter.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Author: Oliver
 */
class HugeFileUniqueSorterTest {
    private static final Logger logger = LoggerFactory.getLogger(HugeFileUniqueSorterTest.class);

    private static final String INPUT_TEST_FILE_PATH = COMMON_FOLDER + "inputTest.txt";
    private static final String TEMP_FOLDER_PATH = COMMON_FOLDER + "testTemp\\";
    private static final String FINAL_OUTPUT_FILE_PATH = COMMON_FOLDER + "testOutput\\FINAL_Sorted_Words.txt";
    private static final long SIZE_THAT_FITS_ALL = 1000L;
    private static final long SIZE_THAT_FITS_MEMORY = 4L;

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

    @Test
    @Disabled
    void testEnd2EndStepOne_thenInformationTheSame() throws FileNotFoundException {
        // Validate preconditions
        Validate.notBlank(INPUT_TEST_FILE_PATH);
        File inputFile = new File(INPUT_TEST_FILE_PATH);
        Validate.isTrue(inputFile.exists() && inputFile.isFile());
        printFiles(inputFile, 5);
        List<File> smallFiles = hugeFile2SmallFiles(inputFile, TEMP_FOLDER_PATH, SIZE_THAT_FITS_ALL);
        printFiles(smallFiles, 5);

        List<String> lst1 = readStringsFromFile(inputFile);
        List<String> lst2 = readStringsFromFile(smallFiles);
        Collections.sort(lst1);
        Collections.sort(lst2);
        assertEquals(lst1, lst2);
    }

    @Test
    void testEnd2EndTwoTempFiles_thenInformationTheSame() throws FileNotFoundException {
        // Validate preconditions
        Validate.notBlank(INPUT_TEST_FILE_PATH);
        File inputFile = new File(INPUT_TEST_FILE_PATH);
        Validate.isTrue(inputFile.exists() && inputFile.isFile());
        printFiles(inputFile, 5);
        List<File> smallFiles = hugeFile2SmallFiles(inputFile, TEMP_FOLDER_PATH, SIZE_THAT_FITS_MEMORY);
        printFiles(smallFiles, 5);

        List<String> lst1 = readStringsFromFile(inputFile);
        List<String> lst2 = readStringsFromFile(smallFiles);
        Collections.sort(lst1);
        Collections.sort(lst2);
        assertEquals(lst1, lst2);

        Validate.notBlank(FINAL_OUTPUT_FILE_PATH);
        File outputFile = new File(FINAL_OUTPUT_FILE_PATH);
        Validate.notNull(outputFile);
        new HugeFileUniqueSorter().kWayMergingSortedArray(smallFiles, outputFile);
        printFiles(outputFile, 5);

        List<String> lst3 = readStringsFromFile(outputFile);
        Collections.sort(lst3);
        assertEquals(lst1, lst3);
    }

    @AfterAll
    static void cleanUp() {
        logger.info("Test End!\n");
    }

    private List<String> readStringsFromFile(File inputFile) throws FileNotFoundException {
        // Validate preconditions
        Validate.notNull(inputFile);
        Validate.isTrue(inputFile.exists() && inputFile.isFile());

        List<String> lst = new LinkedList<>();
        try (Scanner scanner1 = new Scanner(inputFile)) {
            while (scanner1.hasNext()) {
                for (String s : scanner1.nextLine().split(" ", -1)) {
                    lst.add(s.trim().toLowerCase());
                }
            }
        }
        return lst;
    }

    private List<String> readStringsFromFile(List<File> inputFile) throws FileNotFoundException {
        // Validate preconditions
        Validate.notEmpty(inputFile);

        List<String> lst = new LinkedList<>();
        for (File file : inputFile) {
            lst.addAll(readStringsFromFile(file));
        }
        return lst;
    }
}
