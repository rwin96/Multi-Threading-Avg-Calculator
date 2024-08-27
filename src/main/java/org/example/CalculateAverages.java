package org.example;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CalculateAverages {
    private static final int NUM_THREADS = 6;
    private static final ConcurrentHashMap<String, Double> averages = new ConcurrentHashMap<>();
    private static final Lock lock = new ReentrantLock();
    private static long numberProcessedStudents;
    private static long studentsLen;
    private static long startTime;
    private static long endTime;


    public static void main(String[] args) throws IOException, InterruptedException {

        startTime = System.currentTimeMillis();

        String jsonData = new String(Files.readAllBytes(new File("src/main/resources/jsons/students.json").toPath()));
        JSONArray students = new JSONArray(jsonData);
        studentsLen = students.length();

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        // Daste bandi bmola
        int packSize = students.length() / NUM_THREADS;
        List<List<JSONObject>> packs = new ArrayList<>();
        for (int i = 0; i < students.length(); i += packSize) {
            int end = Math.min(i + packSize, students.length());
            List<JSONObject> pack = new ArrayList<>();
            for (int j = i; j < end; j++) {
                pack.add(students.getJSONObject(j));
            }
            packs.add(pack);
        }


        for (List<JSONObject> pack : packs) {
            executor.submit(new CalculateAverage(pack));
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        Thread writerThread = new Thread(new WriteResultsToFileTask());
        writerThread.start();
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
                double average = sum / grades.length();
                averages.put(uuid, average);
            }
        }
    }

    private static class WriteResultsToFileTask implements Runnable {
        @Override
        public void run() {
            while (true) {

                lock.lock();

                try {
                    for (String uuid : averages.keySet()) {
                        JSONObject student = new JSONObject();
                        student.put("average", averages.get(uuid));
                        student.put("uuid", uuid);
                        FileWriter writer = new FileWriter("results/" + uuid + ".json");
                        writer.write(student.toString());
                        writer.close();
                        numberProcessedStudents++;
                    }

                    averages.clear();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }

                if (numberProcessedStudents == studentsLen) {
                    endTime = System.currentTimeMillis();
                    System.out.println("Time spend: " + (endTime - startTime));
                    System.exit(0);
                }
            }
        }
    }
}
