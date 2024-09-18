package com.example.oraclequery.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class OracleQueryService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public String executeQueryAndGetCsv() {
        String query = "SELECT * FROM your_table"; // Replace with your actual query
        
        return jdbcTemplate.query(query, (ResultSet rs) -> {
            StringBuilder csv = new StringBuilder();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Add header
            List<String> headers = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                headers.add(metaData.getColumnName(i));
            }
            csv.append(String.join(",", headers)).append("\n");

            // Add data rows
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(rs.getString(i));
                }
                csv.append(String.join(",", row.stream().map(this::escapeSpecialCharacters).collect(Collectors.toList()))).append("\n");
            }

            return csv.toString();
        });
    }

    private String escapeSpecialCharacters(String data) {
        String escapedData = data.replaceAll("\\R", " ");
        if (data.contains(",") || data.contains("\"") || data.contains("'")) {
            data = data.replace("\"", "\"\"");
            escapedData = "\"" + data + "\"";
        }
        return escapedData;
    }
}