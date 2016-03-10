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

public class TopicExchangeTest extends BaseConfig {

    private static final String EXCHANGE = "Topic_Test";
    private static final String CONSUMER_QUEUE_1 = "Consumer_Topic_1";
    private static final String CONSUMER_QUEUE_2 = "Consumer_Topic_2";
    private static final Map<String, Integer> STATS = new ConcurrentHashMap<>();

    private Connection connection;

    private Channel topicChannel;

    @Before
    public void initializeQueues() throws IOException, TimeoutException {
        connection = getConnection();
        topicChannel = connection.createChannel();
        topicChannel.exchangeDeclare(EXCHANGE, ExchangeTypes.TOPIC.getName());

        topicChannel.queueDeclare(CONSUMER_QUEUE_1, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(),
                Collections.emptyMap());
        topicChannel.queueDeclare(CONSUMER_QUEUE_2, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(),
                Collections.emptyMap());

        topicChannel.queueBind(CONSUMER_QUEUE_1, EXCHANGE, "users.*.*");
        topicChannel.queueBind(CONSUMER_QUEUE_2, EXCHANGE, "users.#");
    }

    @After
    public void clearQueues() throws IOException, TimeoutException {
        STATS.clear();

        topicChannel.queueDelete(CONSUMER_QUEUE_1);
        topicChannel.queueDelete(CONSUMER_QUEUE_2);
        topicChannel.exchangeDelete(EXCHANGE);

        topicChannel.close();
        connection.close();
    }

    @Test
    public void should_deliver_message_to_2_bound_queues() throws IOException, InterruptedException {
        String message = "Topic_Test_1";
        topicChannel.basicPublish(EXCHANGE, "users.and.others", null, message.getBytes());

        CountDownLatch latch = new CountDownLatch(2);

        // declare consumers
        topicChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.yes(), new CountingConsumer(topicChannel, latch, STATS));
        topicChannel.basicConsume(CONSUMER_QUEUE_2, AUTO_ACK.yes(), new CountingConsumer(topicChannel, latch, STATS));

        latch.await(2, TimeUnit.SECONDS);

        // Both patterns match
        assertThat(STATS.get(message).intValue()).isEqualTo(2);
    }

    @Test
    public void should_deliver_message_to_only_1_queue_because_of_mapping_difference() throws InterruptedException, IOException {
        String message = "Topic_Test_2";
        topicChannel.basicPublish(EXCHANGE, "users.and.others.living.here", null, message.getBytes());

        CountDownLatch latch = new CountDownLatch(2);

        // declare consumers
        topicChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.yes(), new CountingConsumer(topicChannel, latch, STATS));
        topicChannel.basicConsume(CONSUMER_QUEUE_2, AUTO_ACK.yes(), new CountingConsumer(topicChannel, latch, STATS));

        latch.await(2, TimeUnit.SECONDS);

        // The message will be delivered to only one consumer because only "users.#" pattern matches
        // The "users.*.*" pattern would match if the routing key was "users.and.others"
        assertThat(STATS.get(message).intValue()).isEqualTo(1);
    }

    @Test
    public void should_not_deliver_the_message_because_there_are_no_matching_key() throws IOException, InterruptedException {
        String message = "Topic_Test_3";
        topicChannel.basicPublish(EXCHANGE, "me.and.users.living.here", null, message.getBytes());

        CountDownLatch latch = new CountDownLatch(2);

        // declare consumers
        // Neither CONSUMER_QUEUE_1 nor CONSUMER_QUEUE_2 don't have their bound keys
        // matching "me.and.users.living.there" - "users" is declared in the middle
        // instead of the beginning
        topicChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.yes(), new CountingConsumer(topicChannel, latch, STATS));
        topicChannel.basicConsume(CONSUMER_QUEUE_2, AUTO_ACK.yes(), new CountingConsumer(topicChannel, latch, STATS));

        latch.await(2, TimeUnit.SECONDS);

        // The message won't be delivered because there are no consumer bound to (anyWord)users(anyWord) pattern
        assertThat(STATS.containsKey(message)).isFalse();
    }

    @Test
    public void should_not_deliver_the_message_if_message_key_is_shorter_than_routing_key() throws IOException, InterruptedException {
        String message = "Topic_Test_4";
        topicChannel.basicPublish(EXCHANGE, "users.99", null, message.getBytes());

        CountDownLatch latch = new CountDownLatch(1);

        // CONSUMER_QUEUE_2 should match because of use of # substitution
        // but CONSUMER_QUEUE_1 won't match because it expects
        // to have a key users.(anyWord).(anyWord)
        topicChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.yes(), new CountingConsumer(topicChannel, latch, STATS));

        latch.await(2, TimeUnit.SECONDS);

        assertThat(STATS.containsKey(message)).isFalse();
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_fail_on_sending_message_with_too_long_routing_key() throws IOException {
        String message = "Test_bad_routing_key";
        String routingKey = "1";
        for (int i = 2; i < 300; i++) {
            routingKey += "."+i;
        }
        topicChannel.basicPublish(EXCHANGE, routingKey, null, message.getBytes());
    }

}
