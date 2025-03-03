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
}
