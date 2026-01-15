package com.github.dimitryivaniuta.kraftdemo;

import com.github.dimitryivaniuta.kraftdemo.consumer.PartitionedConsumers;
import com.github.dimitryivaniuta.kraftdemo.observe.ObservedMessageStore;
import com.github.dimitryivaniuta.kraftdemo.producer.DemoProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest
class KafkaPartitioningIT {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("kraft_demo")
            .withUsername("kraft")
            .withPassword("kraft");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("spring.datasource.url", postgres::getJdbcUrl);
        r.add("spring.datasource.username", postgres::getUsername);
        r.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    DemoProducer producer;
    @Autowired
    ObservedMessageStore store;

    @BeforeEach
    void setUp() {
        store.reset();
    }

    @Test
    void groupA_consumers_read_expected_partitions_and_groupB_reads_all() throws Exception {
        store.expect(PartitionedConsumers.L_GROUP_A_P0, 1);
        store.expect(PartitionedConsumers.L_GROUP_A_P12, 2);
        store.expect(PartitionedConsumers.L_GROUP_B_ALL, 3);

        producer.send("k0", "v0", 0, UUID.randomUUID());
        producer.send("k1", "v1", 1, UUID.randomUUID());
        producer.send("k2", "v2", 2, UUID.randomUUID());

        assertThat(store.await(PartitionedConsumers.L_GROUP_A_P0, 15, TimeUnit.SECONDS)).isTrue();
        assertThat(store.await(PartitionedConsumers.L_GROUP_A_P12, 15, TimeUnit.SECONDS)).isTrue();
        assertThat(store.await(PartitionedConsumers.L_GROUP_B_ALL, 15, TimeUnit.SECONDS)).isTrue();

        var a0 = store.events(PartitionedConsumers.L_GROUP_A_P0);
        var a12 = store.events(PartitionedConsumers.L_GROUP_A_P12);
        var b = store.events(PartitionedConsumers.L_GROUP_B_ALL);

        assertThat(a0).hasSize(1);
        assertThat(a0.getFirst().partition()).isEqualTo(0);

        assertThat(a12).hasSize(2);
        assertThat(a12).extracting(e -> e.partition()).containsExactlyInAnyOrder(1, 2);

        assertThat(b).hasSize(3);
        assertThat(b).extracting(e -> e.partition()).containsExactlyInAnyOrder(0, 1, 2);
    }
}
