package com.waitingforcode.consuming;

import com.rabbitmq.client.AMQP;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.waitingforcode.util.Mapping.BoolMaps.*;
import static org.assertj.core.api.StrictAssertions.assertThat;

public class HeadersExchangeTest extends BaseConfig {

    private static final String EXCHANGE = "Headers_Test";
    private static final String CONSUMER_QUEUE_1 = "Consumer_Headers_1";
    private static final String CONSUMER_QUEUE_2 = "Consumer_Headers_2";
    private static final String CONSUMER_QUEUE_3 = "Consumer_Headers_3";
    private static final String ALL_ROUTINGS = "";
    private static final String HANDLE_NOW = "now";
    private static final String HANDLE_TOMORROW = "tomorrow";
    private static final String RICH_CLIENT = "Rich_Client";
    private static final Map<String, Integer> STATS = new ConcurrentHashMap<>();

    private Connection connection;

    private Channel headersChannel;

    @Before
    public void initializeQueues() throws IOException, TimeoutException {
        connection = getConnection();
        headersChannel = connection.createChannel();
        headersChannel.exchangeDeclare(EXCHANGE, ExchangeTypes.HEADERS.getName());

        Map<String, Object> urgentArgs = new HashMap<>();
        urgentArgs.put("x-match", "any");
        urgentArgs.put("handle-status", HANDLE_NOW);
        urgentArgs.put("message-author", "none");
        headersChannel.queueDeclare(CONSUMER_QUEUE_1, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(),
                Collections.emptyMap());
        // Note: queues are bound with specific mapping
        headersChannel.queueBind(CONSUMER_QUEUE_1, EXCHANGE, ALL_ROUTINGS, urgentArgs);

        Map<String, Object> tomorrowArgs = new HashMap<>();
        tomorrowArgs.put("x-match", "any");
        tomorrowArgs.put("handle-status", HANDLE_TOMORROW);
        tomorrowArgs.put("message-author", RICH_CLIENT);
        headersChannel.queueDeclare(CONSUMER_QUEUE_2, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(),
                Collections.emptyMap());
        headersChannel.queueBind(CONSUMER_QUEUE_2, EXCHANGE, ALL_ROUTINGS, tomorrowArgs);

        Map<String, Object> strictAndMatching = new HashMap<>();
        // Difference between all and any ? all - all headers must match, any - only one from listed headers must match
        strictAndMatching.put("x-match", "all");
        strictAndMatching.put("handle-status", HANDLE_NOW);
        strictAndMatching.put("message-author", RICH_CLIENT);
        headersChannel.queueDeclare(CONSUMER_QUEUE_3, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(),
                Collections.emptyMap());
        headersChannel.queueBind(CONSUMER_QUEUE_3, EXCHANGE, ALL_ROUTINGS, strictAndMatching);
    }

    @After
    public void clearQueues() throws IOException, TimeoutException {
        STATS.clear();

        headersChannel.exchangeDelete(EXCHANGE);

        headersChannel.close();
        connection.close();
    }

    @Test
    public void should_not_deliver_message_to_strict_queue_because_1_header_is_not_matching() throws IOException, InterruptedException {
        String message = "Test_Headers_1";
        Map<String, Object> msgHeaders = new HashMap<>();
        msgHeaders.put("handle-status", HANDLE_NOW);
        msgHeaders.put("message-author", "future client");
        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().headers(msgHeaders).build();
        headersChannel.basicPublish(EXCHANGE, ALL_ROUTINGS, properties, message.getBytes());

        CountDownLatch latch = new CountDownLatch(1);

        // declare consumers
        headersChannel.basicConsume(CONSUMER_QUEUE_3, AUTO_ACK.yes(), new CountingConsumer(headersChannel, latch, STATS));

        latch.await(2, TimeUnit.SECONDS);

        // Because fanout exchange doesn't take into account any additional criteria, two subscribed consumers
        // receive the message
        assertThat(STATS.containsKey(message)).isFalse();
    }

    @Test
    public void should_deliver_message_to_strict_consumer_where_all_headers_are_matching() throws IOException, InterruptedException {
        String message = "Test_Headers_2";
        Map<String, Object> msgHeaders = new HashMap<>();
        msgHeaders.put("handle-status", HANDLE_NOW);
        msgHeaders.put("message-author", RICH_CLIENT);
        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().headers(msgHeaders).build();
        // Message is published with "x" routing key
        // Note that there are no queues bound with this key
        headersChannel.basicPublish(EXCHANGE, "x", properties, message.getBytes());

        CountDownLatch latch = new CountDownLatch(3);

        // declare consumers
        headersChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.yes(), new CountingConsumer(headersChannel, latch, STATS));
        headersChannel.basicConsume(CONSUMER_QUEUE_2, AUTO_ACK.yes(), new CountingConsumer(headersChannel, latch, STATS));
        headersChannel.basicConsume(CONSUMER_QUEUE_3, AUTO_ACK.yes(), new CountingConsumer(headersChannel, latch, STATS));

        latch.await(2, TimeUnit.SECONDS);

        // All 3 consumers receive the message because 2 of them has x-match=any and 1 x-match=all
        // And there are always at least 1 or all matching headers
        assertThat(STATS.get(message).intValue()).isEqualTo(3);
    }

    @Test
    public void should_deliver_message_where_only_one_header_is_matching() throws IOException, InterruptedException {
        String message = "Test_Headers_3";
        Map<String, Object> msgHeaders = new HashMap<>();
        msgHeaders.put("handle-status", HANDLE_TOMORROW);
        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().headers(msgHeaders).build();
        headersChannel.basicPublish(EXCHANGE, ALL_ROUTINGS, properties, message.getBytes());

        CountDownLatch latch = new CountDownLatch(3);

        // declare consumers
        headersChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.yes(), new CountingConsumer(headersChannel, latch, STATS));
        headersChannel.basicConsume(CONSUMER_QUEUE_2, AUTO_ACK.yes(), new CountingConsumer(headersChannel, latch, STATS));
        headersChannel.basicConsume(CONSUMER_QUEUE_3, AUTO_ACK.yes(), new CountingConsumer(headersChannel, latch, STATS));

        latch.await(2, TimeUnit.SECONDS);

        // Only queue#2 should receive the message
        assertThat(STATS.get(message).intValue()).isEqualTo(1);
    }
}
