package org.acme;

import java.lang.reflect.Type;
import java.util.ArrayDeque;
import java.util.Deque;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.keyed.KeyValueExtractor;
import io.smallrye.reactive.messaging.keyed.Keyed;
import io.smallrye.reactive.messaging.keyed.KeyedMulti;

@ApplicationScoped
public class QuotesResource {

    @Incoming("quotes")
    @Outgoing("processed")
    public Multi<Quote> process(@Keyed(KeySelector.class) KeyedMulti<String, Double> quotes) {
        Log.infof("Initializing state for key %s", quotes.key());
        Deque<Double> latest = new ArrayDeque<>(10);
        return quotes
                .map(r -> {
                    if (latest.size() == 10) {
                        latest.pop();
                    }
                    latest.offer(r);
                    double avg = latest.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
                    Log.infof("Avg. price for %s: %f", quotes.key(), avg);
                    return new Quote(quotes.key(), avg);
                });
    }

    @ApplicationScoped
    public static class KeySelector implements KeyValueExtractor {

        @Override
        public boolean canExtract(Message<?> first, Type keyType, Type valueType) {
            IncomingKafkaRecordMetadata metadata = first.getMetadata(IncomingKafkaRecordMetadata.class).get();
            return metadata.getKey().getClass().equals(keyType) && first.getPayload().getClass().equals(valueType);
        }

        @Override
        public Object extractKey(Message<?> message, Type keyType) {
            return message.getMetadata(IncomingKafkaRecordMetadata.class)
                    .map(IncomingKafkaRecordMetadata::getKey)
                    .orElse("");

        }

        @Override
        public Object extractValue(Message<?> message, Type valueType) {
            return message.getPayload();
        }
    }

}
