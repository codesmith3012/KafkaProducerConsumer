package com.storage.UserDetails.Service;

import com.storage.UserDetails.entity.User;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class UserProducer {

    private static final String TOPIC = "user-data";

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplate;

    public void sendUserToKafka(User user) {  // ✅ Method Name Matches Controller
        kafkaTemplate.send(TOPIC, user.getUsername(), user);
        System.out.println("📩 Sent User: " + user);
    }
    public void sendUserToKafkaWithPartition(User user, int partition) {
        // Create a ProducerRecord and specify the partition number
        ProducerRecord<String, User> record = new ProducerRecord<>(TOPIC, partition, user.getUsername(), user);
        kafkaTemplate.send(record);
        System.out.println("📩 Sent User to Partition " + partition + ": " + user);
    }

}
