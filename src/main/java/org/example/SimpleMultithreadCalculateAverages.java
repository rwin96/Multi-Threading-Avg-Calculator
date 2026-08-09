package org.example;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SimpleMultithreadCalculateAverages {

    private static final int NUM_THREADS = Runtime.getRuntime().availableProcessors();
    private static final ConcurrentHashMap<String, Double> averages = new ConcurrentHashMap<>();


    public static void main(String[] args) throws IOException, InterruptedException {

        long startTime = System.currentTimeMillis();

        File file = new File("src/main/resources/jsons/students.json");
        if (!file.exists()) {
            System.err.println("File does not found: " + file.getAbsolutePath());
        }
        String jsonData = new String(Files.readAllBytes(file.toPath()));
        JSONArray students = new JSONArray(jsonData);

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        int packSize = (int) Math.ceil((double) students.length() / NUM_THREADS);
        for (int i = 0; i < students.length(); i += packSize) {
            int end = Math.min(i + packSize, students.length());
            List<JSONObject> pack = new ArrayList<>();
            for (int j = i; j < end; j++) {
                pack.add(students.getJSONObject(j));
            }
            executor.submit(new CalculateAverage(pack));
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        Thread writerThread = new Thread(new WriteResultsToFileTask());
        writerThread.start();

        long endTime = System.currentTimeMillis();

        System.out.println("Total time: " + (endTime - startTime) + "ms");
    }

    private static class CalculateAverage implements Runnable {
        private final List<JSONObject> chunk;

        public CalculateAverage(List<JSONObject> chunk) {
            this.chunk = chunk;
        }

        @Override
        public void run() {
            for (JSONObject student : chunk) {
                String uuid = student.getString("uuid");
                JSONArray grades = student.getJSONArray("grades");
                double sum = 0;
                for (int j = 0; j < grades.length(); j++) {
                    sum += grades.getDouble(j);
                }

                double average = !grades.isEmpty() ? sum / grades.length() : 0;
                averages.put(uuid, average);
            }
        }
    }

    private static class WriteResultsToFileTask implements Runnable {
        @Override
        public void run() {
            File resultDir = new File("results");
            if (!resultDir.exists()) {
                resultDir.mkdir();
            }

            for (Map.Entry<String, Double> entry : averages.entrySet()) {
                JSONObject studentResult = new JSONObject();
                studentResult.put("uuid", entry.getKey());
                studentResult.put("average", entry.getValue());

                File output = new File(resultDir, entry.getKey() + ".json");

                try (FileWriter writer = new FileWriter(output)) {
                    writer.write(studentResult.toString());
                } catch (IOException e) {
                    System.err.println("Error writing results to file: " + output.getAbsolutePath());
                }
            }
        }
    }
}
