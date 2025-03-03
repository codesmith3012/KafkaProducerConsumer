package com.storage.UserDetails.Controller;

import com.storage.UserDetails.Service.UserConsumer;
import com.storage.UserDetails.Service.UserProducer;
import com.storage.UserDetails.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
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

    // ✅ Fetch user data from multiple partitions with corresponding ranges
    @PostMapping("/partitions/ranges")
    public Map<Integer, List<User>> getUserDataByMultiplePartitionsAndRanges(@RequestBody Map<Integer, Map<String, Integer>> partitionRanges) {
        return userConsumer.getUsersByMultiplePartitionsAndRanges(partitionRanges);
    }
}
