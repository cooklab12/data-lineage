import java.sql.*;
import redis.clients.jedis.Jedis;
import java.util.*;

public class DataSyncOracleToRedis {
    
    private static final String DB_URL = "jdbc:oracle:thin:@//your_oracle_host:port/your_service_name";
    private static final String DB_USER = "your_username";
    private static final String DB_PASSWORD = "your_password";
    private static final String REDIS_HOST = "your_redis_host";
    private static final int REDIS_PORT = 6379;

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            
            // Query Oracle database
            String sql = "select query";
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                
                int totalAccountCount = 0;
                Set<String> distinctProductCodes = new HashSet<>();
                
                while (rs.next()) {
                    totalAccountCount++;
                    distinctProductCodes.add(rs.getString("PRODUCT_CODE"));
                }
                
                // Store results in Redis
                jedis.set("total_account_count", String.valueOf(totalAccountCount));
                jedis.set("distinct_product_code_count", String.valueOf(distinctProductCodes.size()));
                
                // Store individual product code counts
                Map<String, Integer> productCodeCounts = new HashMap<>();
                for (String productCode : distinctProductCodes) {
                    productCodeCounts.put(productCode, 0);
                }
                
                rs.beforeFirst(); // Reset result set to beginning
                while (rs.next()) {
                    String productCode = rs.getString("PRODUCT_CODE");
                    productCodeCounts.put(productCode, productCodeCounts.get(productCode) + 1);
                }
                
                for (Map.Entry<String, Integer> entry : productCodeCounts.entrySet()) {
                    jedis.hset("product_code_counts", entry.getKey(), String.valueOf(entry.getValue()));
                }
                
                System.out.println("Data successfully processed and stored in Redis.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}