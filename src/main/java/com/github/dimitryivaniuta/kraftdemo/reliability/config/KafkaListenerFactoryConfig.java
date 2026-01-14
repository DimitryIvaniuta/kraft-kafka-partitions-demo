package com.github.dimitryivaniuta.kraftdemo.reliability.config;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;

/**
 * Ensures our DefaultErrorHandler is actually attached to all @KafkaListener containers.
 */
@Configuration
@RequiredArgsConstructor
public class KafkaListenerFactoryConfig {

    private final DefaultErrorHandler kafkaErrorHandler;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory
    ) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, consumerFactory);

        factory.setCommonErrorHandler(kafkaErrorHandler);
        // keep ordering per partition (do not use concurrency > 1 in a single listener unless you really need it)
        factory.setConcurrency(1);

        return factory;
    }
}
