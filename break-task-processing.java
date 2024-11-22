import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.hazelcast.map.IMap;
import com.hazelcast.core.HazelcastInstance;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class BreakTaskProcessor {

    @Autowired
    private HazelcastInstance hazelcastInstance;

    public List<BreakTaskDTO> processAndStoreBreakTasks(
        List<BreakDetails> breakDetails, 
        List<TaskDetails> taskDetails
    ) {
        return breakDetails.stream()
            .flatMap(breakDetail -> 
                taskDetails.stream()
                    .filter(task -> task.getAcctKey().equals(breakDetail.getAcctKey()))
                    .map(task -> {
                        BreakTaskDTO dto = new BreakTaskDTO();
                        dto.setUniqueId(UUID.randomUUID().toString());
                        dto.setAcctKey(breakDetail.getAcctKey());
                        dto.setErrorCode(breakDetail.getErrorCode());
                        dto.setTaskId(task.getTaskId());
                        dto.setProcessId(task.getProcessId());
                        
                        // Store in Hazelcast
                        IMap<String, BreakTaskDTO> breakTaskMap = 
                            hazelcastInstance.getMap("breakTaskMap");
                        breakTaskMap.put("combined:" + dto.getUniqueId(), dto);
                        
                        return dto;
                    })
            )
            .collect(Collectors.toList());
    }

    public void updateBreakValidation(String acctKey, String errorCode, String validBreak) {
        IMap<String, BreakTaskDTO> breakTaskMap = hazelcastInstance.getMap("breakTaskMap");
        
        breakTaskMap.values().stream()
            .filter(dto -> 
                dto.getAcctKey().equals(acctKey) && 
                dto.getErrorCode().equals(errorCode)
            )
            .forEach(dto -> {
                dto.setValidBreak(validBreak);
                breakTaskMap.put("combined:" + dto.getUniqueId(), dto);
            });
    }

    public long countDistinctTasksByErrorCode(String errorCode) {
        IMap<String, BreakTaskDTO> breakTaskMap = hazelcastInstance.getMap("breakTaskMap");
        
        return breakTaskMap.values().stream()
            .filter(dto -> dto.getErrorCode().equals(errorCode))
            .map(BreakTaskDTO::getTaskId)
            .distinct()
            .count();
    }

    // DTOs
    public static class BreakDetails {
        private String acctKey;
        private String errorCode;
        // Getters and setters
    }

    public static class TaskDetails {
        private String acctKey;
        private String taskId;
        private String processId;
        // Getters and setters
    }

    public static class BreakTaskDTO {
        private String uniqueId;
        private String acctKey;
        private String errorCode;
        private String taskId;
        private String processId;
        private String validBreak;
        // Getters and setters
    }
}
