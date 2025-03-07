package org.redisson;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Main {
    public static void main(String[] args) {
        RedissonClient redisson = configureRedisson();
        RBlockingQueue<String> blockingQueue = redisson.getBlockingQueue("delayedQueue");
        RDelayedQueue<String> delayedQueue = redisson.getDelayedQueue(blockingQueue);

        Thread queueProcessorThread = new Thread(() -> processQueue(blockingQueue));
        queueProcessorThread.start();
        // runCancellationTest(delayedQueue, args);
        runTests(delayedQueue);

        cleanUp(delayedQueue, redisson);
    }

    private static void runTests(RDelayedQueue<String> delayedQueue) {
        runRetainAllTest(delayedQueue);
        // runRemoveAllTest(delayedQueue);
    }

    private static void runRemoveAllTest(RDelayedQueue<String> delayedQueue){
        delayedQueue.clear();
        List<String> randomStrings = generateRandomStrings(1000, 10);
        List<Long> randomDelays = generateRandomDelayList(1000, 3600, 1000);
        Map<String, Long> idToDelay = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            delayedQueue.offer(randomStrings.get(i), randomDelays.get(i), TimeUnit.SECONDS);
            idToDelay.put(randomStrings.get(i), randomDelays.get(i));
        }
        List<String> selectedIds = selectFromList(randomStrings, 500);
        delayedQueue.removeAll(selectedIds);
        Set<String> remainingIds = new HashSet<>(delayedQueue.readAll());
        Set<String> expectedToBeRemoved = new HashSet<>(selectedIds);
        Set<String> expectedToBeRetained = new HashSet<>(randomStrings);
        Set<String> testSet = new HashSet<>(expectedToBeRemoved);
        testSet.retainAll(remainingIds);
        if (!testSet.isEmpty()) {
            System.out.println("runRemoveAllTest Error: Some IDs that should have been removed are still presentwith count "+ testSet.size() +" : " + testSet);
        }
        testSet = new HashSet<>(expectedToBeRetained);
        testSet.removeAll(remainingIds);
        if (!testSet.isEmpty()) {
            System.out.println("runRemoveAllTest Error: Some IDs that should have been retained are not presentwith count "+ testSet.size() +" : " + testSet);
        }
    }

    private static void runRetainAllTest(RDelayedQueue<String> delayedQueue){
        delayedQueue.clear();
        List<String> randomStrings = generateRandomStrings(1000, 10);
        List<Long> randomDelays = generateRandomDelayList(1000, 3600, 1000);
        Map<String, Long> idToDelay = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            delayedQueue.offer(randomStrings.get(i), randomDelays.get(i), TimeUnit.SECONDS);
            idToDelay.put(randomStrings.get(i), randomDelays.get(i));
        }
        List<String> selectedIds = selectFromList(randomStrings, 500);
        // delayedQueue.retainAll(selectedIds);
        Set<String> remainingIds = new HashSet<>(delayedQueue.readAll());
        Set<String> expectedToBeRetained = new HashSet<>(selectedIds);
        Set<String> expectedToBeRemoved = new HashSet<>(randomStrings);
        expectedToBeRemoved.removeAll(expectedToBeRetained);

        Set<String> testSet = new HashSet<>(expectedToBeRemoved);
        testSet.retainAll(remainingIds);
        if (!testSet.isEmpty()) {
            System.out.println("runRetainAllTest Error: Some IDs that should have been removed are still presentwith count "+ testSet.size() +" : " + testSet);
        }
        testSet = new HashSet<>(expectedToBeRetained);
        testSet.removeAll(remainingIds);
        if (!testSet.isEmpty()) {
            System.out.println("runRetainAllTest Error: Some IDs that should have been retained are not present with count "+ testSet.size() +" : " + testSet);
        }
    }

    private static <V> List<V> selectFromList(List<V> list, int count) {
        List<V> selected = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < count; i++) {
            selected.add(list.get(random.nextInt(list.size())));
        }
        return selected;
    }

    private static List<Long> generateRandomDelayList(int count, long maxDelay, long minDelay) {
        List<Long> randomDelays = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < count; i++) {
            randomDelays.add(((long) random.nextInt((int) maxDelay)) + minDelay);
        }
        return randomDelays;
    }

    private static List<String> generateRandomStrings(int count, int length) {
        List<String> randomStrings = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < count; i++) {
            randomStrings.add(generateRandomString(random, length) + i);
        }
        return randomStrings;
    }


    private static String generateRandomString(Random random, int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(characters.charAt(random.nextInt(characters.length())));
        }
        return sb.toString();
    }

    private static RedissonClient configureRedisson() {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6379");
        return Redisson.create(config);
    }

    private static void runCancellationTest(RDelayedQueue<String> delayedQueue, String[] args){
        if (args.length == 0) {
            System.out.println("Need an argument for the test");
            return;
        }
        long iterations = Long.parseLong(args[0]);
        long startTime = System.currentTimeMillis();
        for(int i=0;i<iterations;i++){
            delayedQueue.offer("id"+i, 500, TimeUnit.SECONDS);
        }
        long insertEndTime = System.currentTimeMillis();
        for(int i=0;i<iterations;i++){
            delayedQueue.remove("id"+i);
        }
        long endTime = System.currentTimeMillis();
        long insertionTime = insertEndTime-startTime;
        long removalTime = endTime-insertEndTime;
        long insertionTimePerRequest = insertionTime*1000/iterations;
        long removalTimePerRequest = removalTime*1000/iterations;
        long totalTime = endTime-startTime;
        long totalTimePerRequest = totalTime*1000/iterations;
        System.out.println("Insertion time: "+insertionTime+" ms");
        System.out.println("Removal time: "+removalTime+" ms");
        System.out.println("Total time: "+totalTime+" ms");
        System.out.println("Insertion time per request: "+insertionTimePerRequest+" us");
        System.out.println("Removal time per request: "+removalTimePerRequest+" us");
        System.out.println("Total time per request: "+totalTimePerRequest+" us");

    }


    private static void processCommands(RDelayedQueue<String> delayedQueue) {
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String command = scanner.next();
            if (command.equals("s")) {
                String id = scanner.next();
                long time = scanner.nextLong();
                delayedQueue.offer(id, time, TimeUnit.SECONDS);
            } else if (command.equals("c")) {
                String id = scanner.next();
                delayedQueue.remove(id);
            }
        }
    }

    private static void processQueue(RBlockingQueue<String> blockingQueue) {
        try {
            while (true) {
                String finishedId = blockingQueue.take();
                System.out.println("Finished: " + finishedId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Queue processing thread interrupted");
        }
    }

    private static void cleanUp(RDelayedQueue<String> delayedQueue, RedissonClient redisson) {
        delayedQueue.destroy();
        redisson.shutdown();
    }
}
