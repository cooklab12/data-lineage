import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

// Enhanced DTO with more attributes
public class EnhancedBreakTaskDTO implements Serializable {
    private String uniqueId;
    private String acctKey;
    private String errorCode;
    private String taskId;
    private String processId;
    private String validBreak;
    private String taskName;
    private String status;
    private String priority;
    private String assignedTo;
    private String department;
    private String category;
    private String subCategory;
    private String createdDate;
    private String modifiedDate;
    private String resolution;
    private String severity;
    private String impact;
    private String businessUnit;
    private String region;
    private String country;
    private String platform;
    private String systemSource;
    private String errorType;
    private String errorDescription;
    private String breakType;
    private String breakSource;
    private String reconciliationDate;
    private String reconciliationStatus;
    private String comments;
    
    // Getters and setters omitted for brevity
}

@Service
public class HazelcastSqlQueryService {
    
    @Autowired
    private HazelcastInstance hazelcastInstance;

    // Create mapping for SQL queries
    public void createMapping() {
        SqlService sqlService = hazelcastInstance.getSql();
        
        String createMapping = """
            CREATE MAPPING IF NOT EXISTS breakTaskMap (
                uniqueId VARCHAR,
                acctKey VARCHAR,
                errorCode VARCHAR,
                taskId VARCHAR,
                processId VARCHAR,
                validBreak VARCHAR,
                taskName VARCHAR,
                status VARCHAR,
                priority VARCHAR,
                assignedTo VARCHAR,
                department VARCHAR,
                category VARCHAR,
                subCategory VARCHAR,
                createdDate VARCHAR,
                modifiedDate VARCHAR,
                resolution VARCHAR,
                severity VARCHAR,
                impact VARCHAR,
                businessUnit VARCHAR,
                region VARCHAR,
                country VARCHAR,
                platform VARCHAR,
                systemSource VARCHAR,
                errorType VARCHAR,
                errorDescription VARCHAR,
                breakType VARCHAR,
                breakSource VARCHAR,
                reconciliationDate VARCHAR,
                reconciliationStatus VARCHAR,
                comments VARCHAR
            )
            TYPE IMap
            OPTIONS (
                'keyFormat' = 'varchar',
                'valueFormat' = 'java'
            )
        """;
        
        sqlService.execute(createMapping);
    }

    // Query methods
    public long countDistinctByTaskNameAndStatus(String taskName, String status) {
        SqlService sqlService = hazelcastInstance.getSql();
        
        String query = """
            SELECT COUNT(DISTINCT taskId) as count
            FROM breakTaskMap
            WHERE taskName = ? AND status = ?
        """;
        
        try (SqlResult result = sqlService.execute(query, taskName, status)) {
            SqlRow row = result.iterator().next();
            return row.getObject("count");
        }
    }

    public List<TaskSummary> getTaskSummaryByDepartment() {
        SqlService sqlService = hazelcastInstance.getSql();
        
        String query = """
            SELECT 
                department,
                COUNT(DISTINCT taskId) as totalTasks,
                COUNT(DISTINCT CASE WHEN status = 'OPEN' THEN taskId END) as openTasks,
                COUNT(DISTINCT CASE WHEN status = 'CLOSED' THEN taskId END) as closedTasks
            FROM breakTaskMap
            GROUP BY department
        """;
        
        List<TaskSummary> summaries = new ArrayList<>();
        try (SqlResult result = sqlService.execute(query)) {
            for (SqlRow row : result) {
                TaskSummary summary = new TaskSummary(
                    row.getObject("department"),
                    row.getObject("totalTasks"),
                    row.getObject("openTasks"),
                    row.getObject("closedTasks")
                );
                summaries.add(summary);
            }
        }
        return summaries;
    }

    public List<BreakAnalysis> analyzeBreaksByErrorType(String startDate, String endDate) {
        SqlService sqlService = hazelcastInstance.getSql();
        
        String query = """
            SELECT 
                errorType,
                breakType,
                COUNT(*) as breakCount,
                COUNT(DISTINCT taskId) as affectedTasks,
                COUNT(DISTINCT businessUnit) as affectedBusinessUnits
            FROM breakTaskMap
            WHERE createdDate BETWEEN ? AND ?
            GROUP BY errorType, breakType
            ORDER BY breakCount DESC
        """;
        
        List<BreakAnalysis> analysis = new ArrayList<>();
        try (SqlResult result = sqlService.execute(query, startDate, endDate)) {
            for (SqlRow row : result) {
                BreakAnalysis breakAnalysis = new BreakAnalysis(
                    row.getObject("errorType"),
                    row.getObject("breakType"),
                    row.getObject("breakCount"),
                    row.getObject("affectedTasks"),
                    row.getObject("affectedBusinessUnits")
                );
                analysis.add(breakAnalysis);
            }
        }
        return analysis;
    }

    // Example usage of complex conditions
    public List<EnhancedBreakTaskDTO> findCriticalBreaks() {
        SqlService sqlService = hazelcastInstance.getSql();
        
        String query = """
            SELECT *
            FROM breakTaskMap
            WHERE severity = 'HIGH'
            AND impact = 'CRITICAL'
            AND status != 'CLOSED'
            AND validBreak = 'Y'
            ORDER BY createdDate DESC
        """;
        
        List<EnhancedBreakTaskDTO> criticalBreaks = new ArrayList<>();
        try (SqlResult result = sqlService.execute(query)) {
            for (SqlRow row : result) {
                EnhancedBreakTaskDTO dto = mapRowToDTO(row);
                criticalBreaks.add(dto);
            }
        }
        return criticalBreaks;
    }

    // Helper class for task summaries
    public static class TaskSummary {
        private final String department;
        private final long totalTasks;
        private final long openTasks;
        private final long closedTasks;

        public TaskSummary(String department, long totalTasks, long openTasks, long closedTasks) {
            this.department = department;
            this.totalTasks = totalTasks;
            this.openTasks = openTasks;
            this.closedTasks = closedTasks;
        }
        // Getters omitted for brevity
    }

    // Helper class for break analysis
    public static class BreakAnalysis {
        private final String errorType;
        private final String breakType;
        private final long breakCount;
        private final long affectedTasks;
        private final long affectedBusinessUnits;

        public BreakAnalysis(String errorType, String breakType, long breakCount, 
                           long affectedTasks, long affectedBusinessUnits) {
            this.errorType = errorType;
            this.breakType = breakType;
            this.breakCount = breakCount;
            this.affectedTasks = affectedTasks;
            this.affectedBusinessUnits = affectedBusinessUnits;
        }
        // Getters omitted for brevity
    }

    private EnhancedBreakTaskDTO mapRowToDTO(SqlRow row) {
        EnhancedBreakTaskDTO dto = new EnhancedBreakTaskDTO();
        // Map all fields from row to DTO
        dto.setUniqueId(row.getObject("uniqueId"));
        dto.setAcctKey(row.getObject("acctKey"));
        // ... map other fields
        return dto;
    }
}
