package com.storage.UserDetails.Controller;

import com.storage.UserDetails.Service.UserConsumer;
import com.storage.UserDetails.Service.UserProducer;
import com.storage.UserDetails.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/kafka")
public class UserController {

    @Autowired
    private UserProducer userProducer;

    @Autowired
    private UserConsumer userConsumer;

    // ✅ Send user data to Kafka (Stored based on username in partitions)
    @PostMapping("/send")
    public String sendUserToKafka(@RequestBody User user) {
        userProducer.sendUserToKafka(user);
        return "📩 User sent to Kafka successfully!";
    }

    // ✅ Fetch ALL user data from ALL partitions
    @GetMapping("/all")
    public Map<Integer, List<User>> getAllUserData() {
        return userConsumer.getAllUsersByPartition();
    }

    // ✅ Fetch specific user data from requested partition with a limit on the number of records
    @PostMapping("/partition")
    public Map<Integer, List<User>> getPartitionData(@RequestBody Map<Integer, Integer> partitionData) {
        return userConsumer.getUsersFromPartitions(partitionData);
    }

    @PostMapping("/fetch-with-offset-tracking")
    public ResponseEntity<Map<Integer, List<User>>> getUsersWithOffsetTracking(@RequestBody Map<Integer, Integer> partitionData) {
        Map<Integer, List<User>> users = userConsumer.getUsersWithOffsetTracking(partitionData);
        return ResponseEntity.ok(users);
    }

    // ✅ Fetch user data from multiple partitions with corresponding ranges
    @PostMapping("/partitions/ranges")
    public Map<Integer, List<User>> getUserDataByMultiplePartitionsAndRanges(@RequestBody Map<Integer, Map<String, Integer>> partitionRanges) {
        return userConsumer.getUsersByMultiplePartitionsAndRanges(partitionRanges);
    }
    @PostMapping("/consume-round-robin")
    public String consumeUsersInRoundRobinOrder() {
        // Trigger the round-robin consumption of users from partitions
        userConsumer.consumeUsersInRoundRobinOrderUsingThreads();
        return "✅ Consumption from partitions in round-robin order triggered.";
    }
    @PostMapping("/send-to-partitions")
    public String sendUsersToPartitions() {
        // Send 50 messages to partition 0
        for (int i = 1; i <= 50; i++) {
            User samridhUser = new User("samridh_" + i, "mypassword", "Delhi");
            userProducer.sendUserToKafkaWithPartition(samridhUser, 0);
        }

        // Send 75 messages to partition 1
        for (int i = 1; i <= 75; i++) {
            User tarunUser = new User("tarun_" + i, "mypassword", "Noida");
            userProducer.sendUserToKafkaWithPartition(tarunUser, 1);
        }

        // Send 100 messages to partition 2
        for (int i = 1; i <= 100; i++) {
            User alishaUser = new User("alisha_" + i, "mypassword", "Delhi");
            userProducer.sendUserToKafkaWithPartition(alishaUser, 2);
        }

        return "✅ Users sent to Kafka in specified partitions.";
    }
}
