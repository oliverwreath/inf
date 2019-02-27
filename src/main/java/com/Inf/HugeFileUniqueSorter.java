package com.Inf;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

import static com.Inf.StringUtils.trimNonAlphaDigit;

/**
 * Author: Oliver
 * <p>
 * Input： a huge file with n words (Assumption: n > 100k)
 * Output： Distinct Words in Ascending order (Assumption: We only need alphaDigit words, so remove special characters like , " ' etc...).
 * <p>
 * <p>
 * When all we have is ONE-Small-machine,
 * we are using the sequential I/O so we need some algorithms good for sequential access (like 2-pointers algorithms for merging 2 sorted arrays in linear time/ one pass) and tune it up for production level k-way merging.
 * So for something HUGE, let's use Divide& Conquer methodology to figure this out!
 * <p>
 * Let's define the Chunk means 15G of input data (Assumption: Memory 16G) with n words.
 * Time complexity O(n) for deduplicating one chunk.
 * Time complexity O(n log n) for sorting one chunk.
 * So the bottleneck lies in the sorting.
 * <p>
 * 1st Pass. Very efficient - linear Time Complexity: O(n) - break down into small chunks that fits the memory - linear Space Complexity: O(n).
 * 2nd Pass. Sort each chunk individually.
 * 3rd Pass. Time Complexity: O(n) n is the total number of words; Combine them easily, like, k-way merge at the same time like the 2-pointer algorithms trick, we just need a pointer for every chunk and a get the smallest among them, append to the output, rinse and repeat. (hint Oliver: be careful about deduplicating. This can be a Gotcha moment!)
 * <p>
 * Bonus point/ Optimization for highest standard:
 * 1. Use a good quicksort implementation that can switch to selection sort when the task becomes small.
 * 2. We can ask follow up for TopK and see if he can use minimum heap or TopK algorithms to stand out.
 * <p>
 * <p>
 * Digress: If we can use Big data infrastructures,
 * 1. MapReduce mapper can use a hashSet deduplicating, then reducer can sort.
 */
public class HugeFileUniqueSorter {
    private static final Logger logger = LoggerFactory.getLogger(HugeFileUniqueSorter.class);

    private static final String COMMON_FOLDER = "C:\\Users\\yanli\\IdeaProjects\\inf\\src\\main\\java\\com\\Inf\\";

    private static final String INPUT_SMALL_FILE_PATH = COMMON_FOLDER + "inputSmall.txt";
    private static final String INPUT_HUGE_FILE_PATH = COMMON_FOLDER + "inputHuge.txt";
    private static final String TEMP_FOLDER_PATH = COMMON_FOLDER + "temp\\";
    private static final String FINAL_OUTPUT_FILE_PATH = COMMON_FOLDER + "output\\FINAL_Sorted_Words.txt";
    private static final long SIZE_THAT_FITS_MEMORY = 10000L;
    private static final String FILE_SUFFIX = ".txt";

    /**
     * Step #1 - 1st Pass - break down into small chunks that fits the memory
     *
     * @param inputFile             source of our data (words)
     * @param OUTPUT_FOLDER_PATH    folder to write intermediate data (results)
     * @param SIZE_THAT_FITS_MEMORY the appropriate size of your choice (by number of distinct words)
     * @return list of small chunks' fileNames e.g. chunk1.txt, chunk2.txt, chunk3.txt
     */
    public static List<File> hugeFile2SmallFiles(final File inputFile, final String OUTPUT_FOLDER_PATH, final long SIZE_THAT_FITS_MEMORY) {
        // Validate Preconditions
        Validate.notNull(inputFile);
        Validate.isTrue(inputFile.exists() && inputFile.isFile(), "inputFile %s NOT exists!", inputFile);
        Validate.notBlank(OUTPUT_FOLDER_PATH);
        Validate.isTrue(SIZE_THAT_FITS_MEMORY > 0);
        File file = new File(OUTPUT_FOLDER_PATH);
        emptyOrCreateFolder(file);

        // main logic
        HashSet<String> hashSet = new HashSet<>();
        int fileNumber = 0;
        List<File> smallFiles = new LinkedList<>();
        final String hugeFileName = inputFile.getName().substring(0, inputFile.getName().lastIndexOf('.'));
        try (BufferedReader bf = new BufferedReader(new FileReader(inputFile))) {
            // for Every File
            String line;
            while ((line = bf.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }

                for (String word : line.split(" ", -1)) {
                    // for Every Word
                    String tmpWord = trimNonAlphaDigit(word);
                    if (StringUtils.isNotBlank(tmpWord) || hashSet.contains(tmpWord)) {
                        continue;
                    }

                    hashSet.add(tmpWord);
                    if (hashSet.size() >= SIZE_THAT_FITS_MEMORY) {
                        // Enough to write and flush to a chunk file (file that fits the memory (size configurable)).
                        File smallFile = writeWords2File(hashSet, OUTPUT_FOLDER_PATH + hugeFileName + "_chunk_" + fileNumber++ + FILE_SUFFIX);
                        smallFiles.add(smallFile);
                        hashSet = new HashSet<>();
                    }
                }
            }

            if (hashSet.size() > 0) {
                // Don't forget the remaining words, write and flush to a chunk file.
                File smallFile = writeWords2File(hashSet, OUTPUT_FOLDER_PATH + hugeFileName + "_chunk_" + fileNumber + FILE_SUFFIX);
                smallFiles.add(smallFile);
            }
        } catch (IOException e) {
            logger.error("ERROR reading file: {} {}", inputFile, e);
        }

        return smallFiles;
    }

    /**
     * helper function - helping step 1& 3 by persisting the words into a file of your choice.
     *
     * @param iterableCollection words to persist
     * @param OUTPUT_FILE_PATH   where to persist
     * @return where the data (words) is persisted
     * @throws IOException
     */
    static File writeWords2File(final List<String> iterableCollection, final String OUTPUT_FILE_PATH) {
        // Validate Preconditions
        Validate.notEmpty(iterableCollection);
        Validate.notBlank(OUTPUT_FILE_PATH);

        File outputFile = new File(OUTPUT_FILE_PATH);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
            for (String s : iterableCollection) {
                bw.write(s);
                bw.newLine();
            }
            bw.flush();
        } catch (IOException e) {
            logger.error("ERROR writing outputFile: {}; e: {}", OUTPUT_FILE_PATH, e);
        }
        return outputFile;
    }

    /**
     * helper function - helping step 1& 3 by persisting the words into a file of your choice.
     *
     * @param hashSet          words to persist
     * @param OUTPUT_FILE_PATH where to persist
     * @return where the data (words) is persisted
     */
    static File writeWords2File(final HashSet<String> hashSet, final String OUTPUT_FILE_PATH) {
        // Validate Preconditions
        Validate.notEmpty(hashSet);
        Validate.notBlank(OUTPUT_FILE_PATH);

        return writeWords2File(sortWords(hashSet), OUTPUT_FILE_PATH);
    }

    /**
     * Step #2 - Sort the words.
     *
     * @param hashSet unsorted words.
     * @return sorted words.
     */
    static List<String> sortWords(final HashSet<String> hashSet) {
        // Validate Preconditions
        Validate.notEmpty(hashSet);

        List<String> list = new ArrayList<>(hashSet);
        list.sort((o1, o2) -> o1.compareTo(o2));
        return list;
    }

    /**
     * Step #3 - 3rd Pass - k-way merging efficiently (mimicking classic trick "merging 2 sorted arrays")
     *
     * @param smallFiles a list of k smallFiles (chunks)
     * @param outputFile the final output file (k-way merged file (with deduplication))
     */
    public void kWayMerging(final List<File> smallFiles, final File outputFile) {
        // Validate Preconditions
        Validate.notEmpty(smallFiles);
        Validate.notBlank(FINAL_OUTPUT_FILE_PATH);

        // main logic - k-way merging (by the way handling duplications)
        // first round populate the empty PriorityQueue
        PriorityQueue<Entry> pq = new PriorityQueue<>((o1, o2) -> o1.word.compareTo(o2.word));
        HashMap<Integer, Scanner> scannerMap = new HashMap<>();
        populatePriorityQueueAndScannerMap(smallFiles, pq, scannerMap);
        logger.info("pq = {}", pq);

        // in a loop keep exhausted every source in a k-way merging style
        String lastWord = "";// for FINAL deduplication
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
            while (!pq.isEmpty()) {
                Entry nextEntry = pq.poll();
                if (!lastWord.equals(nextEntry.word)) {
                    lastWord = nextEntry.word;
                    try {
                        bw.write(nextEntry.word);
                        bw.newLine();
                    } catch (IOException e) {
                        logger.error("can't write to finalOutputFile with outputFile: {}; error: {}", outputFile, e);
                    }
                }

                if (scannerMap.containsKey(nextEntry.index)) {
                    Scanner scanner = scannerMap.get(nextEntry.index);
                    if (scanner.hasNext()) {
                        pq.add(new Entry(scanner.next(), nextEntry.index));
                    } else {
                        scannerMap.remove(nextEntry.index);
                    }
                }
            }

            bw.flush();
        } catch (IOException e) {
            logger.error("can't creat or write to finalOutputFile with outputFile: {}; error: {}", outputFile, e);
        }

        return;
    }

    /**
     * helper function - for populating the priorityQueue - for efficient k-way merging
     *
     * @param smallFiles
     * @param pq
     * @param map
     */
    private void populatePriorityQueueAndScannerMap(final List<File> smallFiles, PriorityQueue<Entry> pq, HashMap<Integer, Scanner> map) {
        // Validate Preconditions
        Validate.notEmpty(smallFiles);
        Validate.notNull(pq);
        Validate.notNull(map);

        int index = 0;
        for (File oneFile : smallFiles) {
            try {
                Scanner scanner = new Scanner(oneFile);
                if (scanner.hasNext()) {
                    map.put(index, scanner);
                    pq.add(new Entry(scanner.next(), index));
                    index++;
                }
            } catch (FileNotFoundException e) {
                logger.error("can't find file with path: {} {}", oneFile, e);
            }
        }
    }

    /**
     * helper function that prints multiple files beautifully.
     *
     * @param smallFiles
     * @param topK
     */
    private static long printFiles(final List<File> smallFiles, final int topK) {
        // Validate Preconditions
        Validate.notEmpty(smallFiles);
        Validate.isTrue(topK >= 0);

        logger.info("printFiles: smallFiles = {}", smallFiles + "\n");
        long totalCount = 0;
        for (File filePath : smallFiles) {
            logger.info("filePath = {}", filePath);
            long counter = printFiles(filePath, topK);
            totalCount += counter;
            logger.info("counter = {}", counter + "\n");
        }
        logger.info("totalCountOfAllFiles = {}", totalCount + "\n");
        return totalCount;
    }

    /**
     * helper function that prints a file content beautifully.
     *
     * @param file2Print
     * @param topK
     * @return
     */
    private static long printFiles(final File file2Print, final int topK) {
        // Validate Preconditions
        Validate.notNull(file2Print);
        Validate.isTrue(topK >= 0);

        logger.info("printFiles: file2Print = {}", file2Print);
        long counter = 0;
        try (BufferedReader bf = new BufferedReader(new FileReader(file2Print))) {
            String line;
            while ((line = bf.readLine()) != null) {
                if (!StringUtils.isNotBlank(line)) {
                    if (counter < topK) {
                        logger.info("{}", line);
                    }
                    counter++;
                }
            }
        } catch (IOException e) {
            logger.error("IOException with file2Print: {} {}", file2Print, e);
        }
        logger.info("counter = {}", counter + "\n");
        return counter;
    }

    private static BufferedReader getBufferedReaderForFile(final String filePath) throws FileNotFoundException {
        // Validate Preconditions
        Validate.notBlank(filePath);

        return new BufferedReader(new FileReader(filePath));
    }

    private static boolean emptyOrCreateFolder(final File folder) {
        // Validate Preconditions
        Validate.notNull(folder);

        try {
            FileUtils.deleteDirectory(folder);
        } catch (IOException e) {
            logger.error("ERROR deleting directory: {} {}", folder, e);
        }
        return createFolderIfNotExists(folder);
    }

    private static boolean createFolderIfNotExists(final File folder) {
        // Validate Preconditions
        Validate.notNull(folder);

        return folder.mkdirs();
    }

    private class Entry {
        String word;
        int index;

        Entry(String word, int index) {
            this.word = word;
            this.index = index;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "word='" + word + '\'' +
                    ", index=" + index +
                    '}';
        }
    }

    private static void step1Test() {
        Validate.notBlank(INPUT_HUGE_FILE_PATH);
        File inputFile = new File(INPUT_HUGE_FILE_PATH);
        Validate.isTrue(inputFile.exists() && inputFile.isFile());
        List<File> smallFiles = hugeFile2SmallFiles(inputFile, TEMP_FOLDER_PATH, SIZE_THAT_FITS_MEMORY);
        printFiles(smallFiles, 5);
    }

    private static void step123Test() {
        Validate.notBlank(INPUT_HUGE_FILE_PATH);
        File inputFile = new File(INPUT_HUGE_FILE_PATH);
        Validate.isTrue(inputFile.exists() && inputFile.isFile());
        List<File> smallFiles = hugeFile2SmallFiles(inputFile, TEMP_FOLDER_PATH, SIZE_THAT_FITS_MEMORY);
        printFiles(smallFiles, 5);

        Validate.notBlank(FINAL_OUTPUT_FILE_PATH);
        File outputFile = new File(FINAL_OUTPUT_FILE_PATH);
        Validate.notNull(outputFile);
        new HugeFileUniqueSorter().kWayMerging(smallFiles, outputFile);
        printFiles(outputFile, 5);
    }

    public static void main(String[] args) {
        long startTime = System.nanoTime();

//        step1Test();
        step123Test();

        long endTime = System.nanoTime();
        long duration = (endTime - startTime);  //divide by 1000000 to get milliseconds.
        logger.info("duration = {} seconds", (double) duration / 1000000000);
    }
}
