package org.acme;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.jboss.resteasy.reactive.RestStreamElementType;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.Record;

@Path("/quotes")
public class QuotesResource {

    @Channel("quotes")
    @OnOverflow(OnOverflow.Strategy.LATEST)
    Multi<Record<String, Double>> quotes;

    @GET
    @RestStreamElementType(MediaType.APPLICATION_JSON)
    public Multi<Quote> stream() {
        return quotes.map(record -> new Quote(record.key(), record.value()));
    }
}
