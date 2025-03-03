package com.storage.UserDetails.Partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class UserPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String username = (String) key;
        int numPartitions = cluster.partitionCountForTopic(topic);

        // Assign partitions based on username manually
        switch (username.toLowerCase()) {
            case "samridh": return 0;
            case "tarun": return 1;
            case "alisha": return 2;
            default: return Math.abs(username.hashCode()) % numPartitions; // Default hashing
        }
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}