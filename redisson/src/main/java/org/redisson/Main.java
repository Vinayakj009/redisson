package org.redisson;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        RedissonClient redisson = configureRedisson();
        RBlockingQueue<String> blockingQueue = redisson.getBlockingQueue("delayedQueue");
        RDelayedQueue<String> delayedQueue = redisson.getDelayedQueue(blockingQueue);

        Thread queueProcessorThread = new Thread(() -> processQueue(blockingQueue));
        queueProcessorThread.start();

        // processCommands(delayedQueue);
        runCancellationTest(delayedQueue, args);

        cleanUp(delayedQueue, redisson);
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
