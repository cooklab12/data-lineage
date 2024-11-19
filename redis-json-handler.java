// Model class to represent the break details
package com.example.redis.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.index.Indexed;
import java.time.LocalDateTime;
import java.util.Map;

@RedisHash("BreakRecord")
public class BreakRecord {
    @Id
    private Long uniqueId;
    
    @Indexed
    private Long acctKey;
    
    @Indexed
    private String ppCd;
    
    @Indexed
    private String busDt;
    
    @Indexed
    private String frequency;
    
    @Indexed
    private String taskId;
    
    @Indexed
    private String processId;
    
    @Indexed
    private String taskName;
    
    @Indexed
    private String taskStatus;
    
    @Indexed
    private LocalDateTime processStartTime;
    
    @Indexed
    private String severity;
    
    @Indexed
    private String source;
    
    @Indexed
    private String destination;
    
    @Indexed
    private String breakDescription;
    
    @Indexed
    private String breakCode;
    
    private Map<String, Object> breakDetails;
    
    @Indexed
    private String breakCategory;
    
    @Indexed
    private String taskOwner;
    
    @Indexed
    private String lastReconDt;
    
    @Indexed
    private String securityId;

    // Getters and Setters (omitted for brevity but should be implemented)
}

// Redis Configuration
package com.example.redis.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
@EnableRedisRepositories
public class RedisConfig {
    
    @Bean
    public RedisConnectionFactory connectionFactory() {
        return new LettuceConnectionFactory("localhost", 6379);
    }
    
    @Bean
    public RedisTemplate<String, BreakRecord> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, BreakRecord> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        
        // Use Jackson serializer for values
        template.setValueSerializer(new Jackson2JsonRedisSerializer<>(BreakRecord.class));
        template.setHashValueSerializer(new Jackson2JsonRedisSerializer<>(BreakRecord.class));
        
        // Use String serializer for keys
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        
        template.afterPropertiesSet();
        return template;
    }
}

// Service class to handle Redis operations
package com.example.redis.service;

import com.example.redis.model.BreakRecord;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class BreakRecordService {
    
    private final RedisTemplate<String, BreakRecord> redisTemplate;
    private final ObjectMapper objectMapper;
    
    public BreakRecordService(RedisTemplate<String, BreakRecord> redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }
    
    // Upload JSON data to Redis
    public void uploadJsonData(String jsonContent) throws Exception {
        List<BreakRecord> records = objectMapper.readValue(jsonContent, 
            new TypeReference<List<BreakRecord>>() {});
            
        for (BreakRecord record : records) {
            String key = "break:record:" + record.getUniqueId();
            redisTemplate.opsForValue().set(key, record);
        }
    }
    
    // Search by any attribute
    public List<BreakRecord> searchByAttribute(String attribute, Object value) {
        Set<String> keys = redisTemplate.keys("break:record:*");
        return keys.stream()
            .map(key -> redisTemplate.opsForValue().get(key))
            .filter(record -> {
                try {
                    Object fieldValue = BreakRecord.class.getDeclaredField(attribute)
                        .get(record);
                    return value.equals(fieldValue);
                } catch (Exception e) {
                    return false;
                }
            })
            .collect(Collectors.toList());
    }
    
    // Get distinct account keys by severity and break category
    public Set<Long> getDistinctAccountKeys(String severity, String breakCategory) {
        Set<String> keys = redisTemplate.keys("break:record:*");
        return keys.stream()
            .map(key -> redisTemplate.opsForValue().get(key))
            .filter(record -> severity.equals(record.getSeverity()) 
                && breakCategory.equals(record.getBreakCategory()))
            .map(BreakRecord::getAcctKey)
            .collect(Collectors.toSet());
    }
}

// Example usage in a Controller or Main class
package com.example.redis.controller;

import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;

@RestController
@RequestMapping("/api/breaks")
public class BreakRecordController {
    
    @Autowired
    private BreakRecordService breakRecordService;
    
    @PostMapping("/upload")
    public void uploadData(@RequestBody String jsonContent) throws Exception {
        breakRecordService.uploadJsonData(jsonContent);
    }
    
    @GetMapping("/search")
    public List<BreakRecord> searchRecords(
            @RequestParam String attribute, 
            @RequestParam String value) {
        return breakRecordService.searchByAttribute(attribute, value);
    }
    
    @GetMapping("/aggregate")
    public Set<Long> getDistinctAccounts(
            @RequestParam String severity, 
            @RequestParam String breakCategory) {
        return breakRecordService.getDistinctAccountKeys(severity, breakCategory);
    }
}
