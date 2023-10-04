package org.acme;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.DisabledOnIntegrationTest;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;

@QuarkusTest
@QuarkusTestResource(KafkaCompanionResource.class)
@DisabledOnIntegrationTest
class MyReactiveMessagingApplicationTest {

    @Inject
    MyReactiveMessagingApplication app;

    @InjectKafkaCompanion
    KafkaCompanion companion;

    @Test
    void test() {
        assertThat(companion.consumeStrings().fromTopics("words", 5).awaitCompletion())
                .extracting(ConsumerRecord::value)
                .containsExactly("hello", "from", "quarkus", "reactive", "messaging");
        await().untilAsserted(
                () -> assertThat(app.processed()).containsExactly("HELLO", "FROM", "QUARKUS", "REACTIVE", "MESSAGING"));
    }
}
