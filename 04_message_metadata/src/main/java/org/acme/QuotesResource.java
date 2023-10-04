package org.acme;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

@ApplicationScoped
public class QuotesResource {

    @Incoming("quotes")
    @Outgoing("processed")
    public String process(Double quote, IncomingKafkaRecordMetadata<String, Double> metadata, TracingMetadata tracing) {
        System.out.println(tracing.getCurrentContext());
        Log.infof("Processing %s with price %f", metadata.getKey(), quote);
        return metadata.getKey() + ":" + quote;
    }

    @Incoming("processed")
    public void sink(Quote quote) {
        System.out.println(quote);
        if (quote.price() > 100.0) {
            throw new RuntimeException("Quote price cannot exceed 100.0");
        }
    }

}
