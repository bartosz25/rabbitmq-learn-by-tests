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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.waitingforcode.util.Mapping.BoolMaps.*;
import static org.assertj.core.api.Assertions.assertThat;

public class PrioritiesTest extends BaseConfig {

    private static final String EXCHANGE = "Prioritized_Test";
    private static final String CONSUMER_QUEUE_MAX_5 = "Consumer_Max_5";
    private static final String CONSUMER_QUEUE_MAX_10 = "Consumer_Max_10";
    private static final Map<String, Integer> STATS = new ConcurrentHashMap<>();

    private Connection connection;

    private Channel prioritizedChannel;

    @Before
    public void initializeQueues() throws IOException, TimeoutException {
        connection = getConnection();
        prioritizedChannel = connection.createChannel();
        prioritizedChannel.exchangeDeclare(EXCHANGE, ExchangeTypes.DIRECT.getName());

        Map<String, Object> params1 = new HashMap<>();
        params1.put("x-max-priority", 5);
        prioritizedChannel.queueDeclare(CONSUMER_QUEUE_MAX_5, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(), params1);
        prioritizedChannel.queueBind(CONSUMER_QUEUE_MAX_5, EXCHANGE, "");

        prioritizedChannel.queueDeclare(CONSUMER_QUEUE_MAX_10, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.not(), null);
        prioritizedChannel.queueBind(CONSUMER_QUEUE_MAX_10, EXCHANGE, "");
    }

    @After
    public void clearQueues() throws IOException, TimeoutException {
        STATS.clear();

        prioritizedChannel.queueDelete(CONSUMER_QUEUE_MAX_5);
        prioritizedChannel.queueDelete(CONSUMER_QUEUE_MAX_10);
        prioritizedChannel.exchangeDelete(EXCHANGE);

        prioritizedChannel.close();
        connection.close();
    }

    @Test
    public void should_correctly_order_messages_respecting_max_priority() throws IOException {
        String messageContent1 = "Priority_1";
        prioritizedChannel.basicPublish(EXCHANGE, "", basicProps(1), messageContent1.getBytes());
        String messageContent3 = "Priority_3";
        prioritizedChannel.basicPublish(EXCHANGE, "", basicProps(3), messageContent3.getBytes());
        String messageContent2 = "Priority_2";
        prioritizedChannel.basicPublish(EXCHANGE, "", basicProps(2), messageContent2.getBytes());

        String receivedMsg1 = new String(prioritizedChannel.basicGet(CONSUMER_QUEUE_MAX_5, true).getBody());
        String receivedMsg2 = new String(prioritizedChannel.basicGet(CONSUMER_QUEUE_MAX_5, true).getBody());
        String receivedMsg3 = new String(prioritizedChannel.basicGet(CONSUMER_QUEUE_MAX_5, true).getBody());

        // Expected order: "Priority_3", "Priority_1"
        assertThat(receivedMsg1).isEqualTo(messageContent3);
        assertThat(receivedMsg2).isEqualTo(messageContent2);
        assertThat(receivedMsg3).isEqualTo(messageContent1);
    }

    @Test
    public void should_correctly_order_messages_with_higher_priority_than_max_priority() throws IOException {
        String messageContent3 = "Priority_3";
        prioritizedChannel.basicPublish(EXCHANGE, "", basicProps(3), messageContent3.getBytes());
        // 6 is higher than max allowed priority (5); it's re-prioritized down to the max allowed priority value
        String messageContent6 = "Priority_6";
        prioritizedChannel.basicPublish(EXCHANGE, "", basicProps(6), messageContent6.getBytes());
        String messageContent1 = "Priority_1";
        prioritizedChannel.basicPublish(EXCHANGE, "", basicProps(1), messageContent1.getBytes());
        String messageContent7 = "Priority_7";
        prioritizedChannel.basicPublish(EXCHANGE, "", basicProps(7), messageContent7.getBytes());
        String messageContent9 = "Priority_9";
        prioritizedChannel.basicPublish(EXCHANGE, "", basicProps(9), messageContent9.getBytes());
        String messageContent16 = "Priority_16";
        prioritizedChannel.basicPublish(EXCHANGE, "", basicProps(16), messageContent16.getBytes());
        String messageContent17 = "Priority_17";
        prioritizedChannel.basicPublish(EXCHANGE, "", basicProps(17), messageContent17.getBytes());
        String messageContent21 = "Priority_21";
        prioritizedChannel.basicPublish(EXCHANGE, "", basicProps(21), messageContent21.getBytes());
        String messageContent20 = "Priority_20";
        prioritizedChannel.basicPublish(EXCHANGE, "", basicProps(21), messageContent20.getBytes());
        String messageContent22 = "Priority_22";
        prioritizedChannel.basicPublish(EXCHANGE, "", basicProps(22), messageContent22.getBytes());

        String receivedMsg1 = new String(prioritizedChannel.basicGet(CONSUMER_QUEUE_MAX_5, true).getBody());
        String receivedMsg2 = new String(prioritizedChannel.basicGet(CONSUMER_QUEUE_MAX_5, true).getBody());
        String receivedMsg3 = new String(prioritizedChannel.basicGet(CONSUMER_QUEUE_MAX_5, true).getBody());
        String receivedMsg4 = new String(prioritizedChannel.basicGet(CONSUMER_QUEUE_MAX_5, true).getBody());
        String receivedMsg5 = new String(prioritizedChannel.basicGet(CONSUMER_QUEUE_MAX_5, true).getBody());
        String receivedMsg6 = new String(prioritizedChannel.basicGet(CONSUMER_QUEUE_MAX_5, true).getBody());
        String receivedMsg7 = new String(prioritizedChannel.basicGet(CONSUMER_QUEUE_MAX_5, true).getBody());
        String receivedMsg8 = new String(prioritizedChannel.basicGet(CONSUMER_QUEUE_MAX_5, true).getBody());
        String receivedMsg9 = new String(prioritizedChannel.basicGet(CONSUMER_QUEUE_MAX_5, true).getBody());
        String receivedMsg10 = new String(prioritizedChannel.basicGet(CONSUMER_QUEUE_MAX_5, true).getBody());

        // As you could see, we pushed a lot of message with priority higher
        // than queue's max priority (5)
        // In this case, they will be received through publication date.
        // Older messages are got first.
        assertThat(receivedMsg1).isEqualTo(messageContent6);
        assertThat(receivedMsg2).isEqualTo(messageContent7);
        assertThat(receivedMsg3).isEqualTo(messageContent9);
        assertThat(receivedMsg4).isEqualTo(messageContent16);
        assertThat(receivedMsg5).isEqualTo(messageContent17);
        assertThat(receivedMsg6).isEqualTo(messageContent21);
        assertThat(receivedMsg7).isEqualTo(messageContent20);
        assertThat(receivedMsg8).isEqualTo(messageContent22);
        assertThat(receivedMsg9).isEqualTo(messageContent3);
        assertThat(receivedMsg10).isEqualTo(messageContent1);
    }

    @Test
    public void should_dispatch_messages_always_to_consumer_with_the_highest_priority() throws IOException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        Map<String, Object> args5 = new HashMap<>();
        // switch to 15 to see that associated consumer will handle all messages
        args5.put("x-priority", 5);
        Map<String, Object> args10 = new HashMap<>();
        args10.put("x-priority", 10);

        Map<String, Integer> stats5 = new HashMap<>();
        Map<String, Integer> stats10 = new HashMap<>();

        prioritizedChannel.basicConsume(CONSUMER_QUEUE_MAX_10, AUTO_ACK.yes(), args5, new CountingConsumer(prioritizedChannel, latch, stats5));
        prioritizedChannel.basicConsume(CONSUMER_QUEUE_MAX_10, AUTO_ACK.yes(), args10, new CountingConsumer(prioritizedChannel, latch, stats10));


        prioritizedChannel.basicPublish(EXCHANGE, "", null, "Test_1".getBytes());
        prioritizedChannel.basicPublish(EXCHANGE, "", null, "Test_2".getBytes());
        prioritizedChannel.basicPublish(EXCHANGE, "", null, "Test_3".getBytes());

        latch.await(4, TimeUnit.SECONDS);

        assertThat(stats5).hasSize(0);
        assertThat(stats10).hasSize(3);
    }

    @Test
    public void should_allow_consumer_with_lower_priority_to_handle_messages_when_consumer_having_the_highest_priority_is_blocking() throws IOException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(Integer.MAX_VALUE);
        Map<String, Object> args5 = new HashMap<>();
        args5.put("x-priority", 5);
        Map<String, Object> args10 = new HashMap<>();
        args10.put("x-priority", 10);

        Map<String, Integer> stats5 = new HashMap<>();
        Map<String, Integer> stats10 = new HashMap<>();

        /**
         * "Consumer priorities allow you to ensure that high priority consumers receive messages while they are active,
         * with messages only going to lower priority consumers when the high priority consumers block."
         * <a href="https://www.rabbitmq.com/consumer-priority.html" target="_blank">Source</a>
         *
         * So, expected scenario is:
         * 1) There are 2 consumers: first with the priority of 5, the second with the priority of 10
         * 2) Because there are only 1 allowed unacknowledged message, RabbitMQ should consider consumer unacknowledging
         *    messages as "blocked".
         * 3) Two remaining published messages, "Test_2" and "Test_3", should be delivered to the 2nd consumer, which
         *    acknowledges messages manually after receiving them.
         */
        PotentiallyBlockingConsumer blockingConsumer = new PotentiallyBlockingConsumer(prioritizedChannel, latch, stats10, true);
        PotentiallyBlockingConsumer notBlockingConsumer = new PotentiallyBlockingConsumer(prioritizedChannel, latch, stats5, false);
        // basicQos(...) - only 1 unacknowledged message can be sent to consumers in this channel
        prioritizedChannel.basicQos(1);
        prioritizedChannel.basicConsume(CONSUMER_QUEUE_MAX_10, AUTO_ACK.not(), args5, notBlockingConsumer);
        prioritizedChannel.basicConsume(CONSUMER_QUEUE_MAX_10, AUTO_ACK.not(), args10, blockingConsumer);

        prioritizedChannel.basicPublish(EXCHANGE, "", null, "Test_1".getBytes());
        prioritizedChannel.basicPublish(EXCHANGE, "", null, "Test_2".getBytes());
        prioritizedChannel.basicPublish(EXCHANGE, "", null, "Test_3".getBytes());

        latch.await(3, TimeUnit.SECONDS);

        assertThat(stats5).hasSize(2);
        assertThat(stats5.values()).containsOnly(1);
        assertThat(stats5.keySet()).containsOnly("Test_2", "Test_3");
        assertThat(stats10).hasSize(0);
        assertThat(blockingConsumer.getBlockingMessages()).hasSize(1);
        assertThat(blockingConsumer.getBlockingMessages()).containsOnly("Test_1");
    }

    private static AMQP.BasicProperties basicProps(int priority) {
        return new AMQP.BasicProperties
                .Builder()
                .priority(priority)
                .build();
    }

    private static class PotentiallyBlockingConsumer extends DefaultConsumer {

        private final Map<String, Integer> counter;
        private final CountDownLatch latch;
        private final boolean isBlocking;
        private Set<String> blockingMessages = new HashSet<>();

        public PotentiallyBlockingConsumer(Channel channel, CountDownLatch latch, Map<String, Integer> counter, boolean isBlocking) {
            super(channel);
            this.counter = counter;
            this.isBlocking = isBlocking;
            this.latch = latch;
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                   byte[] body) throws IOException {
            if (isBlocking) {
                blockingMessages.add(new String(body, "UTF-8"));
                // message is rejected; But thanks to "true" flag, RabbitMQ tries to redeliver it, in occurrence to the same
                // consumer as the one which rejected the message
                getChannel().basicReject(envelope.getDeliveryTag(), true);
            } else {
                String message = new String(body, "UTF-8");
                Integer times = counter.get(message);
                if (times == null ) {
                    times = 0;
                }
                counter.put(message, times+1);
                getChannel().basicAck(envelope.getDeliveryTag(), true);
            }
            latch.countDown();
        }

        private Set<String> getBlockingMessages() {
            return blockingMessages;
        }
    }

}
