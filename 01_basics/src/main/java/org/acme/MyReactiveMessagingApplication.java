package org.acme;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class MyReactiveMessagingApplication {

    /**
     *
     */
    @Outgoing("words-out")
    Multi<String> generate() {
        return Multi.createFrom().items("hello", "from", "quarkus", "reactive", "messaging");
    }

    /**
     * Consume the message from the "words-in" channel, uppercase it and send it to the uppercase channel.
     * Messages come from the broker.
     **/
    @Incoming("words-in")
    @Outgoing("uppercase")
    public String toUpperCase(String message) {
        return message.toUpperCase();
    }

    List<String> processed = new CopyOnWriteArrayList<>();

    /**
     *
     */
    @Incoming("uppercase")
    void sink(String message) {
        System.out.println(">> " + message);
        processed.add(message);
    }

    public List<String> processed() {
        return processed;
    }
}
