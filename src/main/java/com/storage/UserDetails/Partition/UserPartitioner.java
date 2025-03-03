package com.storage.UserDetails.Partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class UserPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (key == null) {
            // Handle the case where the key is null (for example, return a random partition or a default partition)
            return 0;
        }

        String username = (String) key;
        int numPartitions = cluster.partitionCountForTopic(topic);

        // Assign partitions based on username manually
        switch (username.toLowerCase()) {
            case "samridh": return 0;
            case "tarun": return 1;
            case "alisha": return 2;
            default:
                // Default partitioning based on hash of username
                return Math.abs(username.hashCode()) % numPartitions;
        }
    }

    @Override
    public void close() {
        // You can close resources if any are initialized in the partitioner
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // You can initialize any configuration here if needed
    }
}
