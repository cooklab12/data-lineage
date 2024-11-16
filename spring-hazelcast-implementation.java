// ProductController.java
package com.example.controller;

import com.example.model.Product;
import com.example.service.ProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/products")
public class ProductController {
    
    @Autowired
    private ProductService productService;
    
    @PostMapping
    public ResponseEntity<Product> createProduct(@RequestBody Product product) {
        Product savedProduct = productService.saveProduct(product);
        return ResponseEntity.ok(savedProduct);
    }
    
    @GetMapping("/{key}")
    public ResponseEntity<Product> getProduct(@PathVariable String key) {
        return productService.getProduct(key)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }
}

// ProductService.java
package com.example.service;

import com.example.model.Product;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class ProductService {
    
    private final IMap<String, Product> productCache;
    
    @Autowired
    public ProductService(HazelcastInstance hazelcastInstance) {
        this.productCache = hazelcastInstance.getMap("products");
    }
    
    public Product saveProduct(Product product) {
        productCache.put(product.getKey(), product);
        return product;
    }
    
    public Optional<Product> getProduct(String key) {
        return Optional.ofNullable(productCache.get(key));
    }
}

// Application.java
package com.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class HazelcastDemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(HazelcastDemoApplication.class, args);
    }
}
