import java.sql.*;

public class ParquetAnalyzer {
    public static void main(String[] args) {
        String parquetFilePath = "/path/to/your/file.parquet";

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            // Create a table from the Parquet file
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE parquet_data AS SELECT * FROM read_parquet('" + parquetFilePath + "')");
            }

            // Get column information
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("PRAGMA table_info('parquet_data')");
                while (rs.next()) {
                    String columnName = rs.getString("name");
                    String columnType = rs.getString("type");
                    System.out.println("Column: " + columnName + ", Type: " + columnType);
                }
            }

            // Analyze each column
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT * FROM parquet_data LIMIT 0");
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    System.out.println("\nAnalyzing column: " + columnName);

                    // Count total rows
                    ResultSet countRs = stmt.executeQuery("SELECT COUNT(*) FROM parquet_data");
                    countRs.next();
                    long totalRows = countRs.getLong(1);
                    System.out.println("Total rows: " + totalRows);

                    // Count distinct values
                    ResultSet distinctRs = stmt.executeQuery("SELECT COUNT(DISTINCT " + columnName + ") FROM parquet_data");
                    distinctRs.next();
                    long distinctCount = distinctRs.getLong(1);
                    System.out.println("Distinct values: " + distinctCount);

                    // Count null values
                    ResultSet nullRs = stmt.executeQuery("SELECT COUNT(*) FROM parquet_data WHERE " + columnName + " IS NULL");
                    nullRs.next();
                    long nullCount = nullRs.getLong(1);
                    System.out.println("Null values: " + nullCount);

                    // Get min and max values (for numeric columns)
                    if (metaData.getColumnType(i) == Types.INTEGER || metaData.getColumnType(i) == Types.BIGINT || 
                        metaData.getColumnType(i) == Types.FLOAT || metaData.getColumnType(i) == Types.DOUBLE) {
                        ResultSet minMaxRs = stmt.executeQuery("SELECT MIN(" + columnName + "), MAX(" + columnName + ") FROM parquet_data");
                        minMaxRs.next();
                        System.out.println("Min value: " + minMaxRs.getString(1));
                        System.out.println("Max value: " + minMaxRs.getString(2));
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}