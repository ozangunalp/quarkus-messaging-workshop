package org.acme;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;

@ApplicationScoped
public class QuotesResource {

    @Incoming("quotes")
    @Outgoing("processed")
    public Message<Quote> process(KafkaRecord<String, Double> quote) {
        Log.infof("Processing %s with price %f", quote.getKey(), quote.getPayload());
        return quote.withPayload(new Quote(quote.getKey(), quote.getPayload()));
    }

    @Incoming("processed")
    public CompletionStage<Void> sink(Message<Quote> quote) {
        if (quote.getPayload().price() > 100.0) {
            return quote.nack(new RuntimeException("Quote price cannot exceed 100.0"));
        }
        return quote.ack();
    }

}
