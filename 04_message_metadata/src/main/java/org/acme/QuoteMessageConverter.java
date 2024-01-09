package org.acme;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;

@ApplicationScoped
public class QuoteMessageConverter implements MessageConverter {

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        System.out.println("CONVERT " + target);
        return target.equals(Quote.class);
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        String s = (String) in.getPayload();
        String[] split = s.split(":");
        return in.withPayload(new Quote(split[0], Double.parseDouble(split[1])));
    }
}
