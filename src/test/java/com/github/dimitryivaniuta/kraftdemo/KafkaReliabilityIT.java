package com.github.dimitryivaniuta.kraftdemo;

import com.github.dimitryivaniuta.kraftdemo.persistence.repo.BusinessEventRepository;
import com.github.dimitryivaniuta.kraftdemo.producer.DemoProducer;
import com.github.dimitryivaniuta.kraftdemo.reliability.service.InboxService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest
class KafkaReliabilityIT {

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

    @Autowired DemoProducer producer;
    @Autowired BusinessEventRepository businessRepo;
    @Autowired InboxService inbox;
    @Autowired JdbcTemplate jdbc;

    @Test
    void dedup_prevents_duplicate_side_effects_per_group() throws Exception {
        UUID eventId = UUID.randomUUID();

        producer.send("k0", "hello", 0, eventId);
        producer.send("k0", "hello-duplicate", 0, eventId); // same eventId => duplicate delivery

        // Both groups should process the event exactly once => 2 rows (group-a + group-b)
        waitUntil(() -> {
            Integer cnt = jdbc.queryForObject(
                    "select count(*) from business_event where event_id = ?",
                    Integer.class,
                    eventId
            );
            assertThat(cnt).isEqualTo(2);
        }, 15_000);
    }

    @Test
    void poison_message_is_stored_and_consumer_continues() throws Exception {
        UUID poisonId = UUID.randomUUID();
        producer.send("kp", "POISON", 1, poisonId);

        waitUntil(() -> {
            Integer poisonCnt = jdbc.queryForObject(
                    "select count(*) from kafka_poison_message where event_id = ?",
                    Integer.class,
                    poisonId
            );
            assertThat(poisonCnt).isEqualTo(2); // one per group (group-a + group-b)
        }, 20_000);

        // Next message in same partition should still be processed (offset advanced)
        UUID okId = UUID.randomUUID();
        producer.send("k-ok", "ok", 1, okId);

        waitUntil(() -> {
            Integer cnt = jdbc.queryForObject(
                    "select count(*) from business_event where event_id = ?",
                    Integer.class,
                    okId
            );
            assertThat(cnt).isEqualTo(2);
        }, 20_000);
    }

    @Test
    void transient_errors_are_retried_and_eventually_processed() throws Exception {
        UUID flakyId = UUID.randomUUID();
        producer.send("kf", "FLAKY", 2, flakyId);

        waitUntil(() -> {
            Integer cnt = jdbc.queryForObject(
                    "select count(*) from business_event where event_id = ?",
                    Integer.class,
                    flakyId
            );
            assertThat(cnt).isEqualTo(2);
        }, 25_000);

        Integer poisonCnt = jdbc.queryForObject(
                "select count(*) from kafka_poison_message where event_id = ?",
                Integer.class,
                flakyId
        );
        assertThat(poisonCnt).isEqualTo(0);
    }

    private static void waitUntil(Runnable assertion, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        AssertionError last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                assertion.run();
                return;
            } catch (AssertionError e) {
                last = e;
                Thread.sleep(250);
            }
        }
        if (last != null) throw last;
    }
}
