package com.example.oraclequery.controller;

import com.example.oraclequery.service.OracleQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class OracleQueryController {

    @Autowired
    private OracleQueryService oracleQueryService;

    @GetMapping("/query")
    public ResponseEntity<String> executeQuery() {
        String csvResult = oracleQueryService.executeQueryAndGetCsv();
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        headers.setContentDispositionFormData("attachment", "query_result.csv");
        
        return ResponseEntity.ok()
                .headers(headers)
                .body(csvResult);
    }
}