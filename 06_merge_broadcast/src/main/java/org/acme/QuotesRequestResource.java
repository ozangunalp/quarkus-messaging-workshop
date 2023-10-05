package org.acme;

import java.util.Random;
import java.util.concurrent.CompletableFuture;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;

@ApplicationScoped
@Path("/quotes")
public class QuotesRequestResource {

    @Channel("quotes-request")
    MutinyEmitter<Double> quotes;

    Random random = new Random();

    @POST
    @Path("request")
    @Consumes(MediaType.TEXT_PLAIN)
    public Uni<Void> request(String product) {
        return quotes.sendMessage(KafkaRecord.of(product, random.nextDouble(100.0))
                .withAck(() -> {
                    Log.info(product + " acked!");
                    return CompletableFuture.completedFuture(null);
                }));
    }

    @Incoming("quotes-request")
    @Outgoing("quotes-out")
    @Outgoing("prices")
    public Double broadcast(Double quote) {
        return quote;
    }

}
