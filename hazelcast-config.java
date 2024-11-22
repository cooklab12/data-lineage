import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HazelcastConfiguration {

    @Bean
    public Config hazelCastConfig() {
        Config config = new Config();
        
        // Network Configuration
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setPort(5701)
                     .setPortAutoIncrement(true);

        // Map Configuration
        MapConfig breakTaskMapConfig = new MapConfig()
            .setName("breakTaskMap")
            .setMaxSizeConfig(new MaxSizeConfig(200, MaxSizeConfig.MaxSizePolicy.FREE_HEAP_SIZE))
            .setEvictionConfig(new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT)
                .setSize(10000));

        config.addMapConfig(breakTaskMapConfig);

        // Optional: Serialization Configuration
        SerializationConfig serializationConfig = config.getSerializationConfig();
        serializationConfig.addPortableFactory(1, classId -> {
            // Add your portable factory implementation if needed
            return null;
        });

        return config;
    }

    @Bean
    public HazelcastInstance hazelcastInstance(Config config) {
        return Hazelcast.newHazelcastInstance(config);
    }
}
