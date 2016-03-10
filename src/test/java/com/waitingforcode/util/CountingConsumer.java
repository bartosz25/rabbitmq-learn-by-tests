package com.waitingforcode.util;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class CountingConsumer extends DefaultConsumer {

    private CountDownLatch latch;
    private Map<String, Integer> counter;

    public CountingConsumer(Channel channel, CountDownLatch latch, Map<String, Integer> counter) {
        super(channel);
        this.latch = latch;
        this.counter = counter;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                               byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        Integer times = counter.get(message);
        if (times == null ) {
            times = 0;
        }
        counter.put(message, times+1);
        latch.countDown();
    }
}
