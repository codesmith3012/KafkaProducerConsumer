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
        userConsumer.consumeUsersInRoundRobinOrder();
        return "✅ Consumption from partitions in round-robin order triggered.";
    }
}
