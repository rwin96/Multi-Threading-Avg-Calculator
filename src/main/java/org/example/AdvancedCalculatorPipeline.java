package org.example;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class AdvancedCalculatorPipeline {

    record StudentBatch(List<JSONObject> students) {
    }

    record StudentResult(String uuid, double average) {
    }

    private static final StudentBatch POISON_BATCH = new StudentBatch(new ArrayList<>());
    private static final StudentResult POISON_RESULT = new StudentResult("POISON", -1);

    public static void main(String[] args) {

        long start = System.currentTimeMillis();

        File resultDir = new File("results");
        if (!resultDir.exists()) {
            resultDir.mkdir();
        }

        BlockingQueue<StudentBatch> rawQueue = new ArrayBlockingQueue<>(5000);
        BlockingQueue<StudentResult> resultQueue = new ArrayBlockingQueue<>(10000);

        int cpuCores = Runtime.getRuntime().availableProcessors();
        int calculatorCores = cpuCores;
        int writerCores = 4;

        ExecutorService calculatorPool = Executors.newFixedThreadPool(calculatorCores);
        ExecutorService writerPool = Executors.newFixedThreadPool(writerCores);

        for (int i = 0; i < writerCores; i++) {
            writerPool.submit(new WriterTask(resultQueue, resultDir));
        }

        for (int i = 0; i < calculatorCores; i++) {
            calculatorPool.submit(new CalculatorTask(rawQueue, resultQueue));
        }

        try {
            produceData("src/main/resources/jsons/students.json", rawQueue, calculatorCores);

            calculatorPool.shutdown();
            calculatorPool.awaitTermination(1, TimeUnit.HOURS);

            for (int i = 0; i < writerCores; i++) {
                resultQueue.put(POISON_RESULT);
            }

            writerPool.shutdown();
            writerPool.awaitTermination(1, TimeUnit.HOURS);

        } catch (Exception e) {
            System.err.println("Pipeline interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
        }

        long end = System.currentTimeMillis();
        System.out.println("Total time taken: " + (end - start));
    }

    private static void produceData(String filePath, BlockingQueue<StudentBatch> rawQueue, int calculatorCores) throws IOException, InterruptedException {

        File file = new File(filePath);
        if (!file.exists()) {
            throw new RuntimeException("File does not exist: " + file.getAbsolutePath());
        }

        String jsonData = new String(Files.readAllBytes(file.toPath()));
        JSONArray students = new JSONArray(jsonData);

        int batchSize = 100;
        List<JSONObject> currentBatch = new ArrayList<>();

        for (int i = 0; i < students.length(); i++) {
            currentBatch.add(students.getJSONObject(i));
            if (currentBatch.size() == batchSize) {
                rawQueue.put(new StudentBatch(new ArrayList<>(currentBatch)));
                currentBatch.clear();
            }
        }

        if (!currentBatch.isEmpty()) {
            rawQueue.put(new StudentBatch(currentBatch));
        }

        for (int i = 0; i < calculatorCores; i++) {
            rawQueue.put(POISON_BATCH);
        }

    }

    private static class CalculatorTask implements Runnable {
        private final BlockingQueue<StudentBatch> inQueue;
        private final BlockingQueue<StudentResult> outQueue;

        public CalculatorTask(BlockingQueue<StudentBatch> inQueue, BlockingQueue<StudentResult> outQueue) {
            this.inQueue = inQueue;
            this.outQueue = outQueue;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    StudentBatch workingBatch = inQueue.take();
                    if (workingBatch == POISON_BATCH) break;

                    for (JSONObject student : workingBatch.students()) {
                        String uuid = student.getString("uuid");
                        JSONArray grades = student.getJSONArray("grades");

                        double sum = 0;
                        for (int i = 0; i < grades.length(); i++) {
                            sum += grades.getDouble(i);
                        }
                        double average = sum / grades.length();
                        outQueue.put(new StudentResult(uuid, average));
                    }
                }

            } catch (Exception e) {
                Thread.currentThread().interrupt();
            }

        }

    }

    private static class WriterTask implements Runnable {

        private final BlockingQueue<StudentResult> inQueue;
        private final File outputDir;

        public WriterTask(BlockingQueue<StudentResult> inQueue, File outputDir) {
            this.inQueue = inQueue;
            this.outputDir = outputDir;
        }

        @Override
        public void run() {

            while (true) {
                try {
                    StudentResult result = inQueue.take();
                    if (result == POISON_RESULT) break;

                    JSONObject json = new JSONObject();
                    json.put("uuid", result.uuid());
                    json.put("average", result.average());

                    File outFile = new File(outputDir, result.uuid() + ".json");
                    try (FileWriter fw = new FileWriter(outFile)) {
                        fw.write(outFile.toString());
                    } catch (IOException e) {
                        System.err.println("Error writing results to file: " + outFile.getAbsolutePath());
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

            }

        }

    }

}
