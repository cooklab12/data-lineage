// Product.java
package com.example.model;

import lombok.Data;
import java.io.Serializable;

@Data
public class Product implements Serializable {
    private String key;
    private String name;
    private String description;
    private Double price;
    private Integer quantity;
    private String category;
}

// HazelcastConfig.java
package com.example.config;

import com.hazelcast.config.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HazelcastConfig {
    
    @Bean
    public Config hazelcastConfig() {
        Config config = new Config();
        config.setInstanceName("hazelcast-instance");
        
        NetworkConfig network = config.getNetworkConfig();
        network.setPort(5701);
        network.setPortAutoIncrement(true);
        
        JoinConfig join = network.getJoin();
        join.getTcpIpConfig().setEnabled(true);
        join.getTcpIpConfig().addMember("localhost"); // Add all server IPs here
        join.getMulticastConfig().setEnabled(false);
        
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("products");
        mapConfig.setEvictionConfig(new EvictionConfig()
            .setEvictionPolicy(EvictionPolicy.LRU)
            .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
            .setSize(10000));
        
        config.addMapConfig(mapConfig);
        
        return config;
    }
}

// application.properties
server.port=9003
spring.application.name=hazelcast-demo
