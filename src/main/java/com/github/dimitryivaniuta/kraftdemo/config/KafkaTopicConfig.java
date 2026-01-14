package com.github.dimitryivaniuta.kraftdemo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Creates exactly one topic with 3 partitions.
 * <p>
 * Note: replication factor is 1 because docker-compose runs a single broker.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic demoTopic(@Value("${app.topic.name}") String topic) {
        return TopicBuilder.name(topic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
