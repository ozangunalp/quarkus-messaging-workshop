package org.acme;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.quarkus.logging.Log;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.common.vertx.ContextLocals;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;

@ApplicationScoped
public class QuotesResource {

    @Incoming("quotes")
    @Outgoing("processed")
    @Blocking(value = "quote-worker")
    public Quote process(ConsumerRecord<String, Double> record) {
        ContextLocals.put("record.offset", record.offset());
        Log.infof("Processing %s with price %f", record.key(), record.value());
        return new Quote(record.key(), record.value());
    }

    @Incoming("processed")
    @Outgoing("processed-out")
    @RunOnVirtualThread
    public Quote sink(Quote quote) {
        Log.infof("Record offset %d : %s", ContextLocals.get("record.offset").orElse(null), quote.product());
        if (quote.price() > 100.0) {
            throw new RuntimeException("Quote price cannot exceed 100.0");
        }
        return quote;
    }

}
