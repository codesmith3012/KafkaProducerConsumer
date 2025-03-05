package com.storage.UserDetails.Service;

import com.storage.UserDetails.entity.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class UserConsumer {

    private final Map<Integer, List<User>> partitionUserStore = new ConcurrentHashMap<>();

    // Declare lastConsumedOffset to track the offset for each partition
    private final Map<Integer, Integer> lastConsumedOffset = new ConcurrentHashMap<>();

    // ✅ Kafka Listener: Stores Users in Correct Partitions
    @KafkaListener(topics = "user-data", groupId = "user_group", containerFactory = "kafkaListenerContainerFactory")
    public void consumeUser(ConsumerRecord<String, User> record) {
        int partition = record.partition();
        User user = record.value();

        partitionUserStore.computeIfAbsent(partition, k -> new ArrayList<>()).add(user);
        System.out.println("✅ Processed User: " + user + " | Partition: " + partition);
    }

    public Map<Integer, List<User>> getUsersFromPartitions(Map<Integer, Integer> partitionData) {
        Map<Integer, List<User>> result = new HashMap<>();

        for (Map.Entry<Integer, Integer> entry : partitionData.entrySet()) {
            Integer partitionNo = entry.getKey();
            Integer dataCount = entry.getValue();
            List<User> users = partitionUserStore.getOrDefault(partitionNo, Collections.emptyList());
            result.put(partitionNo, users.subList(0, Math.min(dataCount, users.size())));
        }

        return result;
    }

    // ✅ Fetch Users By Multiple Partitions and Ranges
    public Map<Integer, List<User>> getUsersByMultiplePartitionsAndRanges(Map<Integer, Map<String, Integer>> partitionRanges) {
        Map<Integer, List<User>> result = new HashMap<>();

        for (Map.Entry<Integer, Map<String, Integer>> entry : partitionRanges.entrySet()) {
            int partition = entry.getKey();
            Map<String, Integer> range = entry.getValue();
            int startIndex = range.get("startIndex");
            int endIndex = range.get("endIndex");

            List<User> users = partitionUserStore.getOrDefault(partition, Collections.emptyList());

            // Ensure indices are within valid range
            int fromIndex = Math.max(0, startIndex);
            int toIndex = Math.min(users.size(), endIndex);

            // Add the users for the specific partition and range
            result.put(partition, users.subList(fromIndex, toIndex));
        }

        return result;
    }

    // ✅ Fetch Users From All Partitions
    public Map<Integer, List<User>> getAllUsersByPartition() {
        return partitionUserStore;
    }


    // ✅ Fetch Users from Partitions **with Stateful Offset Tracking**
    public Map<Integer, List<User>> getUsersWithOffsetTracking(Map<Integer, Integer> partitionData) {
        Map<Integer, List<User>> result = new HashMap<>();

        for (Map.Entry<Integer, Integer> entry : partitionData.entrySet()) {
            int partitionNo = entry.getKey();
            int dataCount = entry.getValue();

            List<User> users = partitionUserStore.getOrDefault(partitionNo, Collections.emptyList());

            // ✅ Get last consumed offset
            int lastOffset = lastConsumedOffset.getOrDefault(partitionNo, 0);
            int startIndex = Math.min(lastOffset, users.size());

            // ✅ Calculate endIndex ensuring we don't exceed available data
            int endIndex = Math.min(startIndex + dataCount, users.size());

            // ✅ Update the last consumed offset
            lastConsumedOffset.put(partitionNo, endIndex);

            // ✅ Add fetched users to result
            result.put(partitionNo, users.subList(startIndex, endIndex));
        }

        return result;
    }
    // ✅ Method for Consuming Users in Round-Robin Order Using Threads
    public void consumeUsersInRoundRobinOrderUsingThreads() {
        // List of partition numbers in the round-robin order
        List<Integer> partitionOrder = Arrays.asList(0, 1, 2);

        // Define limits directly in the function
        int limitForPartition0 = 2;  // Partition 0 can process 5 users
        int limitForPartition1 = 1;  // Partition 1 can process 3 users
        int limitForPartition2 = 1;  // Partition 2 can process 4 users

        AtomicBoolean dataRemaining = new AtomicBoolean(true);

        // List to store threads
        List<Thread> threads = new ArrayList<>();

        // Continue until no more data is left in any of the partitions
        while (dataRemaining.get()) {
            dataRemaining.set(false); // Assume no data remaining at the start of each round

            // Create and start threads for each partition
            for (int partition : partitionOrder) {
                final int currentPartition = partition;
                final int limit = (partition == 0) ? limitForPartition0 :
                        (partition == 1) ? limitForPartition1 :
                                limitForPartition2;

                // Create a thread for consuming users from this partition
                Thread partitionThread = new Thread(() -> {
                    List<User> users = partitionUserStore.getOrDefault(currentPartition, Collections.emptyList());
                    int lastOffset = lastConsumedOffset.getOrDefault(currentPartition, 0);

                    // Calculate how many users we can consume for this partition
                    int availableUsers = Math.min(users.size() - lastOffset, limit);

                    if (availableUsers > 0) {
                        dataRemaining.set(true); // Data is still available

                        // Fetch users for this partition and update the offset
                        List<User> usersToConsume = users.subList(lastOffset, lastOffset + availableUsers);
                        Instant now = Instant.now(); // Get current timestamp

                        // Print the timestamp at the second level along with the consumed users
                        System.out.println("✅ Consuming users from Partition " + currentPartition + ": "
                                + usersToConsume + " | Consumed at: " + now.getEpochSecond() + " seconds");

                        lastConsumedOffset.put(currentPartition, lastOffset + availableUsers);
                    } else {
                        // If no data is left to consume in the partition
                        System.out.println("✅ No more users to consume from Partition " + currentPartition);
                    }
                });

                // Start the thread for this partition
                partitionThread.start();
                threads.add(partitionThread);
            }

            // Wait for all threads to finish before proceeding to the next round
            try {
                for (Thread thread : threads) {
                    thread.join();  // Wait for all threads to finish
                }
                threads.clear();  // Clear the thread list for the next round
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Optional sleep or delay if you want a pause between rounds
            try {
                Thread.sleep(1000);  // Sleep for 1 second between rounds
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("✅ All partitions consumed.");
    }


}
