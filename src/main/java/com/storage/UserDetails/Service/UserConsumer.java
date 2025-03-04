package com.storage.UserDetails.Service;

import com.storage.UserDetails.entity.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
    public void consumeUsersInRoundRobinOrder() {
        // This will hold the partition numbers in the round-robin order
        List<Integer> partitionOrder = Arrays.asList(0, 1, 2);

        // Define the limits directly in the function
        int limitForPartition0 = 3;  // Partition 0 can process 5 users
        int limitForPartition1 = 3;  // Partition 1 can process 3 users
        int limitForPartition2 = 3;  // Partition 2 can process 4 users

        boolean dataRemaining = true;

        // Continue until no more data is left in any of the partitions
        while (dataRemaining) {
            dataRemaining = false;

            // Iterate over each partition in round-robin order
            for (int partition : partitionOrder) {
                // Get the users for the current partition
                List<User> users = partitionUserStore.getOrDefault(partition, Collections.emptyList());
                int lastOffset = lastConsumedOffset.getOrDefault(partition, 0);

                // Set limits based on the partition (directly in the function)
                int limit = (partition == 0) ? limitForPartition0 :
                        (partition == 1) ? limitForPartition1 :
                                limitForPartition2;

                // Calculate how many users we can fetch for this partition
                int availableUsers = Math.min(users.size() - lastOffset, limit);

                if (availableUsers > 0) {
                    dataRemaining = true;  // Data is still available

                    // Fetch users for this partition and update the offset
                    List<User> usersToConsume = users.subList(lastOffset, lastOffset + availableUsers);
                    System.out.println("✅ Consuming users from Partition " + partition + ": " + usersToConsume);
                    lastConsumedOffset.put(partition, lastOffset + availableUsers);
                } else {
                    // If no data is left to consume in the partition, just indicate that
                    System.out.println("✅ No more users to consume from Partition " + partition);
                }
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
