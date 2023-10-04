package org.acme;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;

@QuarkusTest
@QuarkusTestResource(KafkaCompanionResource.class)
@TestHTTPEndpoint(QuotesRequestResource.class)
public class QuotesResourceTest {

    @InjectKafkaCompanion
    KafkaCompanion companion;

    @Test
    void test() {
        given().body("chair").when().post("request")
                .then().statusCode(204);
        given().body("table").when().post("request")
                .then().statusCode(204);
        given().body("book").when().post("request")
                .then().statusCode(204);

        assertThat(companion.consumeDoubles().fromTopics("quotes", 3).awaitCompletion())
                .extracting(ConsumerRecord::key)
                .containsExactly("chair", "table", "book");
    }
}
