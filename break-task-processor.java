import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

// Data Transfer Objects
class BreakDetail {
    private String acctKey;
    private String errorCode;

    // Getters and setters
    public String getAcctKey() { return acctKey; }
    public void setAcctKey(String acctKey) { this.acctKey = acctKey; }
    public String getErrorCode() { return errorCode; }
    public void setErrorCode(String errorCode) { this.errorCode = errorCode; }
}

class TaskDetail {
    private String acctKey;
    private String taskId;
    private String proceessId;

    // Getters and setters
    public String getAcctKey() { return acctKey; }
    public void setAcctKey(String acctKey) { this.acctKey = acctKey; }
    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public String getProceessId() { return proceessId; }
    public void setProceessId(String proceessId) { this.proceessId = proceessId; }
}

class CombinedBreakTask {
    private String uniqueId;
    private String acctKey;
    private String errorCode;
    private String taskId;
    private String proceessId;
    private String validBreak;

    // Constructor
    public CombinedBreakTask(BreakDetail breakDetail, TaskDetail taskDetail) {
        this.uniqueId = UUID.randomUUID().toString();
        this.acctKey = breakDetail.getAcctKey();
        this.errorCode = breakDetail.getErrorCode();
        this.taskId = taskDetail.getTaskId();
        this.proceessId = taskDetail.getProceessId();
    }

    // Getters and setters
    public String getUniqueId() { return uniqueId; }
    public String getAcctKey() { return acctKey; }
    public String getErrorCode() { return errorCode; }
    public String getTaskId() { return taskId; }
    public String getProceessId() { return proceessId; }
    public String getValidBreak() { return validBreak; }
    public void setValidBreak(String validBreak) { this.validBreak = validBreak; }
}

@Service
public class BreakTaskProcessor {
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    public List<CombinedBreakTask> combineBreakAndTaskDetails(
        List<BreakDetail> breakDetails, 
        List<TaskDetail> taskDetails
    ) {
        return breakDetails.stream()
            .flatMap(breakDetail -> 
                taskDetails.stream()
                    .filter(taskDetail -> 
                        breakDetail.getAcctKey().equals(taskDetail.getAcctKey()))
                    .map(taskDetail -> new CombinedBreakTask(breakDetail, taskDetail))
            )
            .collect(Collectors.toList());
    }

    public void storeInRedis(CombinedBreakTask combinedBreakTask) {
        // Store in Redis with hashkey as combined:uniqueId
        String hashKey = "combined:" + combinedBreakTask.getUniqueId();
        
        // Convert object to map for Redis storage
        Map<String, Object> redisMap = objectMapper.convertValue(combinedBreakTask, Map.class);
        
        redisTemplate.opsForHash().putAll(hashKey, redisMap);
    }

    public void updateValidBreak(String acctKey, String errorCode) {
        // Find and update records with matching acctKey and errorCode
        List<CombinedBreakTask> matchingTasks = findByAcctKeyAndErrorCode(acctKey, errorCode);
        
        matchingTasks.forEach(task -> {
            task.setValidBreak("Y");
            storeInRedis(task);
        });
    }

    public List<CombinedBreakTask> findByAcctKeyAndErrorCode(String acctKey, String errorCode) {
        // In a real-world scenario, you'd use more efficient Redis querying
        // This is a simplified demonstration
        return Collections.emptyList(); // Placeholder
    }

    public long countDistinctTasksByErrorCode(String errorCode) {
        // Aggregation method to count distinct taskIds for a given errorCode
        // In a real implementation, you'd use Redis aggregation capabilities
        return 0; // Placeholder
    }
}

// Redis Configuration
@Configuration
public class RedisConfig {
    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        
        // Configure serializers
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new GenericToStringSerializer<>(Object.class));
        template.setValueSerializer(new GenericToStringSerializer<>(Object.class));
        
        return template;
    }
}

// Sample Usage in a Controller or Service
@RestController
public class BreakTaskController {
    @Autowired
    private BreakTaskProcessor breakTaskProcessor;

    @PostMapping("/process-breaks")
    public ResponseEntity<String> processBreakTasks(
        @RequestBody Map<String, List> inputData
    ) {
        List<BreakDetail> breakDetails = inputData.get("breakDetails");
        List<TaskDetail> taskDetails = inputData.get("taskDetails");

        List<CombinedBreakTask> combinedTasks = breakTaskProcessor
            .combineBreakAndTaskDetails(breakDetails, taskDetails);

        // Store each combined task in Redis
        combinedTasks.forEach(breakTaskProcessor::storeInRedis);

        return ResponseEntity.ok("Tasks processed successfully");
    }
}
