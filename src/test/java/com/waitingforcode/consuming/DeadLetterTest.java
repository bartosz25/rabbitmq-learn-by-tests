package com.waitingforcode.consuming;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.waitingforcode.BaseConfig;
import com.waitingforcode.util.ExchangeTypes;
import com.waitingforcode.util.CountingConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.waitingforcode.util.Mapping.BoolMaps.*;
import static org.assertj.core.api.Assertions.assertThat;

public class DeadLetterTest extends BaseConfig {

    private static final String DLX_NAME = "MyDeadLetterExchange";
    private static final String EXCHANGE = "Direct_Test";
    private static final String CONSUMER_QUEUE_1 = "Consumer_1";
    private static final String DLX_QUEUE = "Dead_Letter_Queue";
    private static final String DLX_KEY = "New_Key_For_DLX_Msg";
    private static final String DIRECT_EXC_KEY = "Key_1";
    private static final Map<String, Integer> STATS = new ConcurrentHashMap<>();

    private Connection connection;

    private Channel directChannel;

    @Before
    public void initializeQueues() throws IOException, TimeoutException {
        connection = getConnection();
        directChannel = connection.createChannel();
        directChannel.exchangeDeclare(DLX_NAME, ExchangeTypes.FANOUT.getName());
        directChannel.exchangeDeclare(EXCHANGE, ExchangeTypes.FANOUT.getName());

        directChannel.queueDeclare(DLX_QUEUE, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(),
        Collections.emptyMap());
        directChannel.queueBind(DLX_QUEUE, DLX_NAME, "");

        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", DLX_NAME);
        args.put("x-dead-letter-routing-key", DLX_KEY);
        directChannel.queueDeclare(CONSUMER_QUEUE_1, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(), args);
        directChannel.queueBind(CONSUMER_QUEUE_1, EXCHANGE, DIRECT_EXC_KEY);
    }

    @After
    public void clearQueues() throws IOException, TimeoutException {
        STATS.clear();

        directChannel.queueDelete(DLX_QUEUE);
        directChannel.queueDelete(CONSUMER_QUEUE_1);
        directChannel.exchangeDelete(EXCHANGE);
        directChannel.exchangeDelete(DLX_NAME);

        directChannel.close();
        connection.close();
    }

    @Test
    public void should_move_message_to_dlx_because_of_rejecting() throws IOException, InterruptedException {
        String message = "Rejected_message";
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, message.getBytes());

        final CountDownLatch latch = new CountDownLatch(1);
        // message should not be delivered because it expires before thread's awake
        directChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.not(), new DefaultConsumer(directChannel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                // Message should be rejected and go to dead letter queue
                // Note that the consumer is configured with AUTO_ACK.not();
                // otherwise, the message is acknowledged independently
                // of below code and the test doesn't work
                getChannel().basicNack(envelope.getDeliveryTag(), false, false);
                latch.countDown();
            }
        });
        latch.await(1, TimeUnit.SECONDS);
        // Be sure that was not consumed in
        assertThat(STATS.get(message)).isNull();

        CountDownLatch latch2 = new CountDownLatch(1);
        directChannel.basicConsume(DLX_QUEUE, AUTO_ACK.yes(), new CountingConsumer(directChannel, latch2, STATS));
        latch2.await(1, TimeUnit.SECONDS);

        assertThat(STATS.get(message)).isEqualTo(1);
    }

    @Test
    public void should_move_message_to_dlx_because_of_expiration() throws InterruptedException, IOException {
        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().expiration("300").build();
        String message = "Test_300_ms";
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, properties, message.getBytes());

        CountDownLatch latch = new CountDownLatch(1);
        // message should not be delivered because it
        // expires before thread's awake
        Thread.sleep(550);
        directChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.yes(), new CountingConsumer(directChannel, latch, STATS));
        latch.await(1, TimeUnit.SECONDS);
        // Be sure that was not consumed in
        assertThat(STATS.get(message)).isNull();

        latch = new CountDownLatch(1);
        directChannel.basicConsume(DLX_QUEUE, AUTO_ACK.yes(), new CountingConsumer(directChannel, latch, STATS));
        latch.await(1, TimeUnit.SECONDS);

        assertThat(STATS.get(message)).isEqualTo(1);
    }

    @Test
    public void should_correctly_get_information_about_message_routed_to_dead_letter() throws IOException, InterruptedException {
        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().expiration("300").build();
        String message = "Test_300_ms";
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, properties, message.getBytes());

        CountDownLatch latch = new CountDownLatch(1);
        // message should not be delivered because it
        // expires before thread's awake
        Thread.sleep(550);
        directChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.yes(), new CountingConsumer(directChannel, latch, STATS));
        latch.await(1, TimeUnit.SECONDS);
        // Be sure that was not consumed in
        assertThat(STATS.get(message)).isNull();

        StringBuilder headerData = new StringBuilder();
        final CountDownLatch finalLatch = new CountDownLatch(1);
        directChannel.basicConsume(DLX_QUEUE, AUTO_ACK.yes(), new DefaultConsumer(directChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                headerData.append(properties.getHeaders().containsKey("x-death")).append(";");
                ArrayList<HashMap<String, Object>> deadLetterData =
                        (ArrayList<HashMap<String, Object>>) properties.getHeaders().get("x-death");
                headerData.append(deadLetterData.get(0).get("reason").toString()).append(";")
                    .append(deadLetterData.get(0).get("exchange").toString()).append(";")
                    .append(envelope.getRoutingKey());
            }
        });
        finalLatch.await(2, TimeUnit.SECONDS);

        // Expected elements in headerData String are:
        // containsKey("x-death")=true -> x-death is defined in the header
        // reason=expired -> message dead was caused by expired TTL
        // exchange=Direct_Test -> original exchange to which message was sent
        // routingKey=New_Key_For_DLX_Msg -> new routing key set after detection of
        //                                   message "death"
        assertThat(headerData.toString()).isEqualTo("true;expired;Direct_Test;New_Key_For_DLX_Msg");
    }

}
