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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.waitingforcode.util.Mapping.BoolMaps.*;
import static org.assertj.core.api.Assertions.assertThat;

public class TxTransactionTest extends BaseConfig {

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
        // enables transactional mode for this channel
        // without this declaration given error will be thrown when trying to manipulate transaction commit or rollback:
        // "method<channel.close>(reply-code=406, reply-text=PRECONDITION_FAILED -
        // channel is not transactional, class-id=90, method-id=30)"
        directChannel.txSelect();
        directChannel.exchangeDeclare(EXCHANGE, ExchangeTypes.DIRECT.getName());

        directChannel.queueDeclare(CONSUMER_QUEUE_1, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(),
                Collections.emptyMap());
        directChannel.queueBind(CONSUMER_QUEUE_1, EXCHANGE, DIRECT_EXC_KEY);
    }

    @After
    public void clearQueues() throws IOException, TimeoutException {
        directChannel.queueDelete(CONSUMER_QUEUE_1);
        directChannel.exchangeDelete(EXCHANGE);

        directChannel.close();
        connection.close();
    }

    @Test
    public void should_not_consume_published_message_without_transaction_commit() throws IOException, InterruptedException {
        String message = "Test_Not_Committed";
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, message.getBytes());

        CountDownLatch latch = new CountDownLatch(1);

        // declare consumer
        directChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.not(), new CountingConsumer(directChannel, latch, STATS));

        latch.await(2, TimeUnit.SECONDS);

        // If the channel wasn't transactional, we could be able to consume the message - the code is exactly the same as
        // for the DirectExchangeTest
        // But since published message is not committed, RabbitMQ can't consume it
        assertThat(STATS).isEmpty();
    }

    @Test
    public void should_rollback_not_acknowledged_message() throws IOException, InterruptedException, TimeoutException {
        String message = "Test_Not_Ack_Msg";
        STATS.put(message, 0);
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, message.getBytes());
        directChannel.txCommit();

        CountDownLatch latch = new CountDownLatch(2); // 2 because we want to wait 2 seconds before continue
        // declare consumers
        directChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.not(), new DefaultConsumer(directChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String msg = new String(body);
                STATS.put(msg, STATS.get(msg) + 1);
                getChannel().basicNack(envelope.getDeliveryTag(), MULTIPLE.not(), REQUEUE.yes());
                //getChannel().txCommit();
                getChannel().txRollback();
                latch.countDown();
            }
        });
        latch.await(2, TimeUnit.SECONDS);

        // Because we rollback the transaction, not acknowledging should be ignored.
        // To see not acknowledging work, comment txRollback() line and uncomment txCommit() one
        assertThat(STATS.get(message)).isEqualTo(1);
    }

}
