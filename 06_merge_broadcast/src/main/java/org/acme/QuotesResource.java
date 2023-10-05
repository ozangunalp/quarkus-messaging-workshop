package org.acme;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.split.MultiSplitter;
import io.smallrye.reactive.messaging.Targeted;

@ApplicationScoped
public class QuotesResource {

    @Incoming("quotes")
    @Outgoing("processed-product")
    @Outgoing("processed-price")
    @Outgoing("processed")
    public Targeted process(ConsumerRecord<String, Double> record) {
        Log.infof("Processing %s with price %f", record.key(), record.value());
        return Targeted.of("processed-product", record.key())
                .with("processed-price", Math.floor(record.value()))
                .with("processed", new Quote(record.key(), record.value()));
    }

    public enum QuoteSplit {
        LOW,
        HIGH;

        @Override
        public String toString() {
            return "processed-" + name().toLowerCase();
        }
    }

    @Incoming("processed")
    @Outgoing("processed-low")
    @Outgoing("processed-high")
    public MultiSplitter<Quote, QuoteSplit> sink(Multi<Quote> quotes) {
        return quotes.split(QuoteSplit.class, quote -> {
            if (quote.price() < 50.0) {
                return QuoteSplit.LOW;
            } else {
                return QuoteSplit.HIGH;
            }
        });
    }

}
