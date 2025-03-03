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

    // ✅ Fetch ALL Users From ALL Partitions
    public Map<Integer, List<User>> getAllUsersByPartition() {
        return partitionUserStore;
    }

    // ✅ Fetch Limited Users from Selected Partitions
    public Map<Integer, List<User>> getUsersByPartitions(Map<Integer, Integer> partitionLimits) {
        Map<Integer, List<User>> result = new HashMap<>();

        for (Map.Entry<Integer, Integer> entry : partitionLimits.entrySet()) {
            int partition = entry.getKey();
            int limit = entry.getValue();

            List<User> users = partitionUserStore.getOrDefault(partition, Collections.emptyList());
            result.put(partition, users.subList(0, Math.min(limit, users.size())));
        }

        return result;
    }
}
