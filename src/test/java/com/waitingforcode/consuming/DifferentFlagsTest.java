package com.waitingforcode.consuming;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;
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
import static org.assertj.core.api.Fail.fail;

public class DifferentFlagsTest extends BaseConfig {

    private static final String EXCHANGE = "Direct_Test";
    private static final String CONSUMER_QUEUE_1 = "Consumer_1";
    private static final String CONSUMER_QUEUE_1_BIS = "Consumer_1Bis";
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

        directChannel.queueDeclare(CONSUMER_QUEUE_1_BIS, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(),
                Collections.emptyMap());
        directChannel.queueBind(CONSUMER_QUEUE_1_BIS, EXCHANGE, DIRECT_EXC_KEY);
    }

    @After
    public void clearQueues() throws IOException, TimeoutException {
        STATS.clear();
        if (directChannel.isOpen()) {
            directChannel.queueDelete(CONSUMER_QUEUE_1);
            directChannel.queueDelete(CONSUMER_QUEUE_1_BIS);
            directChannel.exchangeDelete(EXCHANGE);

            directChannel.close();
        }
        connection.close();
    }

    @Test
    public void should_return_message_with_mandatory_flag_sent_to_not_existing_queue() throws IOException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        boolean[] wasReturned = new boolean[] {false};
        directChannel.addReturnListener((replyCode, replyText,exchange, routingKey, properties, body) -> {
            wasReturned[0] = true;
            latch.countDown();
        });
        directChannel.basicPublish(EXCHANGE, "Routing_no_queue", MANDATORY.yes(), null, "Test".getBytes());
        latch.await(3, TimeUnit.SECONDS);

        assertThat(wasReturned[0]).isTrue();
    }

    @Test
    public void should_return_message_to_listener_and_reroute_it_manually() throws IOException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        boolean[] wasReturned = new boolean[] {false};
        directChannel.addReturnListener((replyCode, replyText, exchange, routingKey, properties, body) -> {
            wasReturned[0] = true;
            directChannel.basicPublish(exchange, DIRECT_EXC_KEY, properties, body);
            latch.countDown();
        });
        directChannel.basicPublish(EXCHANGE, "Routing_no_queue", MANDATORY.yes(), null, "Test".getBytes());
        latch.await(3, TimeUnit.SECONDS);

        GetResponse msgResponse = directChannel.basicGet(CONSUMER_QUEUE_1, AUTO_ACK.yes());

        assertThat(wasReturned[0]).isTrue();
        assertThat(msgResponse).isNotNull();
        assertThat(new String(msgResponse.getBody())).isEqualTo("Test");
    }

    @Test
    public void should_prevent_against_removing_not_empty_and_used_queue() throws IOException, InterruptedException {
        directChannel.basicPublish(EXCHANGE, CONSUMER_QUEUE_1, MANDATORY.yes(), null, "Test".getBytes());
        Thread sleepingConsumer = new Thread(() -> {
            try {
                directChannel.basicConsume(CONSUMER_QUEUE_1, new DefaultConsumer(directChannel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                               byte[] body) throws IOException {
                        try {
                            // simulates queue "still used"
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        sleepingConsumer.run();
        Thread.sleep(2000);

        try {
            directChannel.queueDelete(CONSUMER_QUEUE_1, IF_UNUSED.yes(), IF_EMPTY.yes());
            fail("Should detect queue as being 'still in use'");
        } catch (IOException io) {
            ShutdownSignalException cause = (ShutdownSignalException) io.getCause();
            assertThat(cause.getReason().toString()).contains("PRECONDITION_FAILED - queue 'Consumer_1' in vhost '/' in use");
            System.out.println("=> "+cause.getReason());
        }
    }

    @Test
    public void should_declare_auto_deletable_queue() throws IOException, InterruptedException {
        String queueName = "Auto_Delete_Queue";
        directChannel.queueDeclare(queueName, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(), null);
        String consumerTag = directChannel.basicConsume(queueName, new DefaultConsumer(directChannel));

        AMQP.Queue.DeclareOk declareOk = directChannel.queueDeclarePassive(queueName);
        assertThat(declareOk.getQueue()).isEqualTo(queueName);

        // We simulate "queue not in use" by removing its consumer
        Thread.sleep(1000);
        directChannel.basicCancel(consumerTag);

        // Now queue passive declaration should fail because of
        // not existent queue
        // By the way, it also illustrates the behaviour of
        // queue passive declaration when the queue exists and not
        Thread.sleep(1000);
        try {
            declareOk = directChannel.queueDeclarePassive(queueName);
            System.out.println("=> "+declareOk.getQueue());
            fail("Should fail on declaring not existent queue passively");
        } catch (IOException ioe) {
            ShutdownSignalException cause = (ShutdownSignalException) ioe.getCause();
            assertThat(cause.toString()).contains("NOT_FOUND - no queue 'Auto_Delete_Queue' in vhost '/'");
        }
    }

    @Test
    public void should_not_access_exclusive_queue_from_other_connection() throws IOException, TimeoutException {
        String queueName = "Exclusive_Queue";
        directChannel.queueDeclare(queueName, DURABLE.not(), EXCLUSIVE.yes(), AUTO_DELETE.yes(), null);

        // Create new connection to see if it can access exclusive queue
        Connection newConnection = CONNECTION_FACTORY.newConnection();
        Channel newChannel = newConnection.createChannel();
        try {
            newChannel.basicConsume(queueName, new DefaultConsumer(newChannel));
            fail("Should not be able to access to exclusive queue");
        } catch (IOException ioe) {
            ShutdownSignalException cause = (ShutdownSignalException) ioe.getCause();
            assertThat(cause.toString()).contains("RESOURCE_LOCKED - cannot obtain exclusive access to " +
                    "locked queue 'Exclusive_Queue' in vhost");
        }
    }

    @Test
    public void should_not_be_able_to_publish_on_internal_exchange() throws IOException {
        String exchangeName = "Internal_Exchange";
        String queueName = "Internal_Queue";
        directChannel.exchangeDeclare(exchangeName, ExchangeTypes.DIRECT.getName(), DURABLE.not(), AUTO_DELETE.yes(),
                INTERNAL.yes(), null);
        directChannel.queueDeclare(queueName, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(),
                Collections.emptyMap());
        directChannel.queueBind(queueName, exchangeName, "x");
        try {
            directChannel.basicPublish(exchangeName, "", null, "Test".getBytes());
            directChannel.basicGet(queueName, true);
            fail("Should not be able to publish on internal queue");
        } catch (IOException ioe) {
            ShutdownSignalException cause = (ShutdownSignalException) ioe.getCause();
            assertThat(cause.toString()).contains("ACCESS_REFUSED - cannot publish " +
                    "to internal exchange 'Internal_Exchange' in vhost '/'");
        }
    }

    @Test
    public void should_deliver_the_message_to_only_exclusive_consumer_even_if_another_one_is_declared() throws IOException, InterruptedException {
        String message = "Test_Exclusive_Consumer";
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, message.getBytes());

        CountDownLatch latch = new CountDownLatch(2);

        // declare consumers - note that we declare two consumers to one queue. It shouldn't made the message
        // being received twice.
        directChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.yes(), "consumer_1",
                NO_LOCAl.not(), EXCLUSIVE.yes(), null, new CountingConsumer(directChannel, latch, STATS));
        try {
            // declaration of new consumer if there are already an exclusive consumer
            // should fail
            directChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.yes(), "consumer_2",
                    NO_LOCAl.not(), EXCLUSIVE.not(), null, new CountingConsumer(directChannel, latch, STATS));
            fail("Should not be able to declare consumer if there are already one exclusive consumer");
        }  catch (IOException ioe) {
            ShutdownSignalException cause = (ShutdownSignalException) ioe.getCause();
            assertThat(cause.toString()).contains("ACCESS_REFUSED - queue 'Consumer_1' in vhost '/' in exclusive use");
        }
        latch.await(2, TimeUnit.SECONDS);

        assertThat(STATS.get(message).intValue()).isEqualTo(1);
    }

    @Test
    public void should_accept_messages_for_no_local_consumer_and_shared_connection_with_publisher() throws IOException, InterruptedException, TimeoutException {
        // According to RabbitMQ specification (https://www.rabbitmq.com/specification.html),
        // no-local is not implemented:
        // "The no-local parameter is not implemented. The value
        // of this parameter is ignored and no attempt is made
        // to prevent a consumer from receiving messages
        // that were published on the same connection."
        // So following test will pass
        CountDownLatch latch = new CountDownLatch(1);
        directChannel.basicConsume(CONSUMER_QUEUE_1, AUTO_ACK.yes(), "consumer_2",
                NO_LOCAl.yes(), EXCLUSIVE.not(), null, new CountingConsumer(directChannel, latch, STATS));
        directChannel.basicPublish(EXCHANGE, DIRECT_EXC_KEY, null, "Test".getBytes());

        latch.await(2, TimeUnit.SECONDS);

        assertThat(STATS).hasSize(1);
    }


}
