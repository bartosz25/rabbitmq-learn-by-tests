package com.waitingforcode.consuming;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.waitingforcode.BaseConfig;
import com.waitingforcode.util.ExchangeTypes;
import com.waitingforcode.util.CountingConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.waitingforcode.util.Mapping.BoolMaps.*;
import static org.assertj.core.api.StrictAssertions.assertThat;

public class DirectExchangeTest extends BaseConfig {

    private static final String EXCHANGE = "Direct_Test";
    private static final String CONSUMER_QUEUE_1 = "Consumer_1";
    private static final String CONSUMER_QUEUE_1_BIS = "Consumer_1Bis";
    private static final String CONSUMER_QUEUE_2 = "Consumer_2";
    private static final String DIRECT_EXC_KEY = "Key_1";
    private static final String NOT_DIRECT_EXC_KEY = "Not_Direct_Key_1";
    private static final Map<String, Integer> STATS = new ConcurrentHashMap<>();

    private Connection connection;

    private Channel directChannel;

    @Before
    public void initializeQueues() throws IOException, TimeoutException {
        connection = getConnection();
        directChannel = connection.createChannel();
        directChannel.exchangeDeclare(EXCHANGE, ExchangeTypes.DIRECT.getName());

        directChannel.queueDeclare(CONSUMER_QUEUE_1, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(),
                Collections.emptyMap());
        directChannel.queueBind(CONSUMER_QUEUE_1, EXCHANGE, DIRECT_EXC_KEY);

        directChannel.queueDeclare(CONSUMER_QUEUE_1_BIS, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(),
                Collections.emptyMap());
        directChannel.queueBind(CONSUMER_QUEUE_1_BIS, EXCHANGE, DIRECT_EXC_KEY);

        directChannel.queueDeclare(CONSUMER_QUEUE_2, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(),
                Collections.emptyMap());
        directChannel.queueBind(CONSUMER_QUEUE_2, EXCHANGE, NOT_DIRECT_EXC_KEY);
    }

    @After
    public void clearQueues() throws IOException, TimeoutException {
        STATS.clear();
        directChannel.queueDelete(CONSUMER_QUEUE_1);
        directChannel.queueDelete(CONSUMER_QUEUE_1_BIS);
        directChannel.queueDelete(CONSUMER_QUEUE_2);
        directChannel.exchangeDelete(EXCHANGE);

        directChannel.close();
        connection.close();
    }

    @Test
    public void should_consume_published_message_only_once() throws IOException, InterruptedException {
        String message = "Test_1";
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, message.getBytes());

        CountDownLatch latch = new CountDownLatch(3);

        // declare consumers - note that we declare two consumers to one queue. It shouldn't made the message
        // being received twice.
        directChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.yes(), new CountingConsumer(directChannel, latch, STATS));
        directChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.yes(), new CountingConsumer(directChannel, latch, STATS));
        directChannel.basicConsume(CONSUMER_QUEUE_2, AUTO_ACK.yes(), new CountingConsumer(directChannel, latch, STATS));

        latch.await(2, TimeUnit.SECONDS);

        // Even if there are 2 consumers defined for Queue#1, the message is received only once
        assertThat(STATS.get(message).intValue()).isEqualTo(1);
    }

    @Test
    public void should_consume_published_message_twice() throws IOException, InterruptedException {
        String message = "Test_2";
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, message.getBytes());

        CountDownLatch latch = new CountDownLatch(3);

        // declare consumers
        directChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.yes(), new CountingConsumer(directChannel, latch, STATS));
        directChannel.basicConsume(CONSUMER_QUEUE_1_BIS, AUTO_ACK.yes(), new CountingConsumer(directChannel, latch, STATS));
        directChannel.basicConsume(CONSUMER_QUEUE_2, AUTO_ACK.yes(), new CountingConsumer(directChannel, latch, STATS));

        latch.await(2, TimeUnit.SECONDS);

        // Two valid queues are bound to direct channel's exchange thanks to DIRECT_EXC_KEY
        assertThat(STATS.get(message).intValue()).isEqualTo(2);
    }


}
