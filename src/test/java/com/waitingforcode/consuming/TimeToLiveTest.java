package com.waitingforcode.consuming;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.ShutdownSignalException;
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
import static org.assertj.core.api.Fail.fail;
import static org.assertj.core.api.Assertions.*;

public class TimeToLiveTest extends BaseConfig {

    private static final String EXCHANGE = "TTL_Test";
    private static final String TESTED_QUEUE = "Consumer_Durable";
    private static final String EXPIRING_QUEUE = "2sec_Expiring_Queue";
    private static final String DIRECT_EXC_KEY = "Key_1";
    private static final Map<String, Integer> STATS = new ConcurrentHashMap<>();

    private Connection connection;

    private Channel directChannel;

    @Before
    public void initializeQueues() throws IOException, TimeoutException {
        connection = getConnection();
        directChannel = connection.createChannel();
        directChannel.exchangeDeclare(EXCHANGE, ExchangeTypes.DIRECT.getName());
    }

    @After
    public void clearQueues() throws IOException, TimeoutException {
        STATS.clear();

        if (directChannel.isOpen()) {
            directChannel.queueDelete(TESTED_QUEUE);
            directChannel.queueDelete(EXPIRING_QUEUE);
            directChannel.exchangeDelete(EXCHANGE);

            directChannel.close();
        }
        connection.close();
    }

    @Test
    public void should_create_queue_and_wait_for_expiring() throws IOException, InterruptedException {
        Map<String, Object> args = new HashMap<>();
        args.put("x-expires", 2000);
        directChannel.queueDeclare(EXPIRING_QUEUE, DURABLE.not(), EXCLUSIVE.not() , AUTO_DELETE.not(), args);
        directChannel.queueBind(EXPIRING_QUEUE, EXCHANGE, DIRECT_EXC_KEY);
        // First, declare consumer, so the queue is in use
        String consumerKey = directChannel.basicConsume(EXPIRING_QUEUE, AUTO_ACK.yes(), new DefaultConsumer(directChannel));
        // Wait some time and delete consumer
        Thread.sleep(3000);
        directChannel.basicCancel(consumerKey);
        // Wait 2.5 seconds and check if the queue is still alive
        Thread.sleep(2500);
        try {
            directChannel.queueDeclarePassive(EXPIRING_QUEUE);
            fail("Should fail when queue is deleted after expiration time");
        } catch (IOException ioe) {
            ShutdownSignalException cause = (ShutdownSignalException)ioe.getCause();
            assertThat(cause.getReason().toString()).contains("NOT_FOUND - " +
                    "no queue '2sec_Expiring_Queue' in vhost '/'");
        }
    }

    @Test
    public void should_send_message_after_queue_expiring() throws IOException, InterruptedException {
        Map<String, Object> args = new HashMap<>();
        args.put("x-expires", 2000);
        directChannel.queueDeclare(EXPIRING_QUEUE, DURABLE.not(), EXCLUSIVE.not() , AUTO_DELETE.not(), args);
        directChannel.queueBind(EXPIRING_QUEUE, EXCHANGE, DIRECT_EXC_KEY);
        // First, declare consumer, so the queue is in use
        String consumerKey = directChannel.basicConsume(EXPIRING_QUEUE, AUTO_ACK.yes(), new DefaultConsumer(directChannel));
        // Wait some time and delete consumer
        Thread.sleep(3000);
        directChannel.basicCancel(consumerKey);
        // Wait 2.5 seconds and check if the queue is still alive
        Thread.sleep(2500);
        // Steps are the same as in previous test
        // Now we add the listener for all returned message. A message can
        // be returned when is transient and there are no queue
        // to receive it . It's our case because 2sec_Expiring_Queue was
        // automatically removed
        final CountDownLatch latch = new CountDownLatch(1);
        boolean[] wasReturned = new boolean[] {false};
        directChannel.addReturnListener((replyCode, replyText,exchange, routingKey, properties, body) -> {
            wasReturned[0] = true;
            latch.countDown();
            System.out.println("For returned message "+new String(body));
        });
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, MANDATORY.yes(), null, "Test".getBytes());

        Thread.sleep(2000);
        assertThat(wasReturned[0]).isTrue();
    }

    @Test
    public void should_not_be_able_to_consume_dead_message_because_of_per_queue_message_ttl_configuration() throws IOException, InterruptedException {
        Map<String, Object> args = new HashMap<>();
        args.put("x-message-ttl", 2000);
        directChannel.queueDeclare(EXPIRING_QUEUE, DURABLE.not(), EXCLUSIVE.not() , AUTO_DELETE.not(), args);
        directChannel.queueBind(EXPIRING_QUEUE, EXCHANGE, DIRECT_EXC_KEY);

        // First, publish message and wait 3 seconds
        // (1 more than in x-message-ttl)
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, MANDATORY.yes(), null, "Test".getBytes());
        Thread.sleep(3000);

        CountDownLatch latch = new CountDownLatch(1);
        directChannel.basicConsume(EXPIRING_QUEUE, AUTO_ACK.yes(), new CountingConsumer(directChannel, latch, STATS));
        latch.await(2, TimeUnit.SECONDS);

        assertThat(STATS).isEmpty();
        // The message shouldn't be accessible by basic.get
        assertThat(directChannel.basicGet(EXPIRING_QUEUE, AUTO_ACK.yes())).isNull();
    }

    @Test
    public void should_use_lower_value_of_two_message_ttl_parameters() throws IOException, InterruptedException {
        Map<String, Object> args = new HashMap<>();
        args.put("x-message-ttl", 60000); // 1 minute of time-to-live
        directChannel.queueDeclare(EXPIRING_QUEUE, DURABLE.not(), EXCLUSIVE.not() , AUTO_DELETE.not(), args);
        directChannel.queueBind(EXPIRING_QUEUE, EXCHANGE, DIRECT_EXC_KEY);

        // First, publish message with 1 second time-to-live
        // This value should be used as TTL parameter for
        // this message.
        // If TTL is specified on queue and message side,
        // lower value of them is used.
        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().expiration("1000").build();
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, MANDATORY.yes(), properties, "Test".getBytes());
        Thread.sleep(2000);

        CountDownLatch latch = new CountDownLatch(1);
        directChannel.basicConsume(EXPIRING_QUEUE, AUTO_ACK.yes(), new CountingConsumer(directChannel, latch, STATS));
        latch.await(2, TimeUnit.SECONDS);

        assertThat(STATS).isEmpty();
        // The message shouldn't be accessible by basic.get
        assertThat(directChannel.basicGet(EXPIRING_QUEUE, AUTO_ACK.yes())).isNull();
    }

    @Test
    public void should_not_handle_expired_message() throws IOException, InterruptedException {
        directChannel.queueDeclare(TESTED_QUEUE, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(),
            Collections.emptyMap());
        directChannel.queueBind(TESTED_QUEUE, EXCHANGE, DIRECT_EXC_KEY);
        // Message builder contains a method called expiration()
        // 300 ms Time-To-Live
        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().expiration("300").build();
        String message = "Test_300_ms";
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, properties, message.getBytes());

        CountDownLatch latch = new CountDownLatch(1);
        // message should not be delivered because it expires before thread's awake
        Thread.sleep(550);
        directChannel.basicConsume(TESTED_QUEUE, AUTO_ACK.yes(), new CountingConsumer(directChannel, latch, STATS));

        latch.await(3, TimeUnit.SECONDS);

        assertThat(STATS.get(message)).isNull();
    }

}
