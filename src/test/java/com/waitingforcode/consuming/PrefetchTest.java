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

public class PrefetchTest extends BaseConfig {

    private static final String EXCHANGE = "Direct_Test";
    private static final String CONSUMER_QUEUE_1 = "Consumer_1";
    private static final String DIRECT_EXC_KEY = "Key_1";
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
    }

    @After
    public void clearQueues() throws IOException, TimeoutException {
        STATS.clear();

        directChannel.queueDelete(CONSUMER_QUEUE_1);
        directChannel.exchangeDelete(EXCHANGE);

        directChannel.close();
        connection.close();
    }


    @Test
    public void should_not_accept_more_messages_than_specified_in_prefetch_count_for_a_single_consumer() throws IOException, InterruptedException {
        String message = "Test_1";
        // max 3 unacknowledged messages per consumer
        directChannel.basicQos(3, GLOBAL_CHANNEL_PREFETCH.not());

        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, message.getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, message.getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, message.getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, message.getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, message.getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, message.getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, message.getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, message.getBytes());

        CountDownLatch latch = new CountDownLatch(8);

        // declare consumer
        directChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.not(), new CountingConsumer(directChannel, latch, STATS));

        latch.await(3, TimeUnit.SECONDS);

        // we allow RabbitMQ to dispatch only 3 messages; and because
        // there are no more than 3 messages acknowledged, given consumer can't
        // consume then more
        // To see that all 8 messages are normally consumed, simply comment basicQos(...) call
        assertThat(STATS.get(message).intValue()).isEqualTo(3);
    }

    @Test
    public void should_block_one_consumer_but_continue_to_dispatch_to_another_one() throws IOException, InterruptedException {
        // This time we try to discover the behaviour when 2 consumers
        // are subscribed to a queue and both can consume
        // only 3 messages without acknowledging them
        directChannel.basicQos(3, GLOBAL_CHANNEL_PREFETCH.not());

        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, testMessage("1").getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, testMessage("2").getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, testMessage("3").getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, testMessage("4").getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, testMessage("5").getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, testMessage("6").getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, testMessage("7").getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, testMessage("8").getBytes());

        CountDownLatch latch = new CountDownLatch(8);

        // declare consumers
        directChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.not(), new CountingConsumer(directChannel, latch, STATS));
        directChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.not(), new CountingConsumer(directChannel, latch, STATS));

        latch.await(3, TimeUnit.SECONDS);

        // we allow RabbitMQ to dispatch only 3 messages per consumer.
        // Because there are 2 available consumers, first 6 messages should be consumed
        // Once again, comment basicQos(...) to see test fail
        assertThat(STATS.get(testMessage("1")).intValue()).isEqualTo(1);
        assertThat(STATS.get(testMessage("2")).intValue()).isEqualTo(1);
        assertThat(STATS.get(testMessage("3")).intValue()).isEqualTo(1);
        assertThat(STATS.get(testMessage("4")).intValue()).isEqualTo(1);
        assertThat(STATS.get(testMessage("5")).intValue()).isEqualTo(1);
        assertThat(STATS.get(testMessage("6")).intValue()).isEqualTo(1);
        assertThat(STATS.get(testMessage("7"))).isNull();
        assertThat(STATS.get(testMessage("8"))).isNull();
    }

    @Test
    public void should_apply_prefetch_globally_to_given_channel() throws IOException, InterruptedException {
        // In this test, the prefetch is applied globally to a channel.
        // So even if we have 2 workers, each accepting 3 messages,
        // the next messages won't be dispatched to not busy workers
        directChannel.basicQos(3, GLOBAL_CHANNEL_PREFETCH.yes());

        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, testMessage("1").getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, testMessage("2").getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, testMessage("3").getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, testMessage("4").getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, testMessage("5").getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, testMessage("6").getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, testMessage("7").getBytes());
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, testMessage("8").getBytes());

        CountDownLatch latch = new CountDownLatch(8);

        // declare consumers; potentially 6 messages consumed
        // but only when prefetch is not global for whole channel
        directChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.not(), new CountingConsumer(directChannel, latch, STATS));
        directChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.not(), new CountingConsumer(directChannel, latch, STATS));

        latch.await(3, TimeUnit.SECONDS);

        // we allow RabbitMQ to dispatch only 3 messages per consumer.
        // Because there are 2 available consumers, first 6 messages
        // should be consumed
        // Once again, comment basicQos(...) to see test fail
        assertThat(STATS.get(testMessage("1")).intValue()).isEqualTo(1);
        assertThat(STATS.get(testMessage("2")).intValue()).isEqualTo(1);
        assertThat(STATS.get(testMessage("3")).intValue()).isEqualTo(1);
        assertThat(STATS.get(testMessage("4"))).isNull();
        assertThat(STATS.get(testMessage("5"))).isNull();
        assertThat(STATS.get(testMessage("6"))).isNull();
        assertThat(STATS.get(testMessage("7"))).isNull();
        assertThat(STATS.get(testMessage("8"))).isNull();
    }

    private String testMessage(String number) {
        return "TestMsg_"+number;
    }

}
