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

public class FanoutExchangeTest extends BaseConfig {

    private static final String EXCHANGE = "Fanout_Test";
    private static final String CONSUMER_QUEUE_1 = "Consumer_1";
    private static final String CONSUMER_QUEUE_2 = "Consumer_2";
    private static final String FANOUT_KEY = "Key_1";
    private static final String NOT_FANOUT_KEY = "Not_Fanout_Key_1";
    private static final Map<String, Integer> STATS = new ConcurrentHashMap<>();

    private Connection connection;

    private Channel fanoutChannel;

    @Before
    public void initializeQueues() throws IOException, TimeoutException {
        connection = getConnection();
        fanoutChannel = connection.createChannel();
        fanoutChannel.exchangeDeclare(EXCHANGE, ExchangeTypes.FANOUT.getName());

        fanoutChannel.queueDeclare(CONSUMER_QUEUE_1, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(),
                Collections.<String, Object>emptyMap());
        fanoutChannel.queueDeclare(CONSUMER_QUEUE_2, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(),
                Collections.<String, Object>emptyMap());

        fanoutChannel.queueBind(CONSUMER_QUEUE_1, EXCHANGE, FANOUT_KEY);
        fanoutChannel.queueBind(CONSUMER_QUEUE_2, EXCHANGE, NOT_FANOUT_KEY);

    }

    @After
    public void clearQueues() throws IOException, TimeoutException {
        fanoutChannel.queueDelete(CONSUMER_QUEUE_1);
        fanoutChannel.queueDelete(CONSUMER_QUEUE_2);
        fanoutChannel.exchangeDelete(EXCHANGE);

        fanoutChannel.close();
        connection.close();
    }

    @Test
    public void should_consume_published_message_twice() throws IOException, InterruptedException {
        String message = "Test_Fanout_1";
        fanoutChannel.basicPublish(EXCHANGE, FANOUT_KEY, null, message.getBytes());

        CountDownLatch latch = new CountDownLatch(2);

        // declare consumers - note that we publish a message with FANOUT_KEY
        // and that the CONSUMER_QUEUE_2 is bound to NOT_FANOUT_KEY
        // If it was direct exchange, this queue shouln't receive the message
        fanoutChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.yes(), new CountingConsumer(fanoutChannel, latch, STATS));
        fanoutChannel.basicConsume(CONSUMER_QUEUE_2, AUTO_ACK.yes(), new CountingConsumer(fanoutChannel, latch, STATS));

        latch.await(2, TimeUnit.SECONDS);

        // Because fanout exchange doesn't take into account any additional criteria, two subscribed consumers
        // receive the message
        assertThat(STATS.get(message).intValue()).isEqualTo(2);
    }


}
