package org.acme;

import java.util.Random;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import io.smallrye.reactive.messaging.kafka.Record;

@ApplicationScoped
@Path("/quotes")
public class QuotesRequestResource {

    @Channel("quotes-request")
    Emitter<Record<String, Double>> quotes;

    Random random = new Random();

    @POST
    @Path("request")
    @Produces()
    @Consumes(MediaType.TEXT_PLAIN)
    public CompletionStage<Void> postRequest(String product) {
        return quotes.send(Record.of(product, random.nextDouble(100.0)));
    }

}
