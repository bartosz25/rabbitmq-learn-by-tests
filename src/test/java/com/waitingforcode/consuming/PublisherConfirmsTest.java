package com.waitingforcode.consuming;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.waitingforcode.BaseConfig;
import com.waitingforcode.util.ExchangeTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

import static com.waitingforcode.util.Mapping.BoolMaps.*;
import static org.assertj.core.api.Assertions.assertThat;

public class PublisherConfirmsTest extends BaseConfig {

    private volatile Set<Long> notConfirmedIds = Collections.synchronizedSet(new TreeSet<>());
    private static final String EXCHANGE = "PublisherConfirms_Test";
    private static final String CONSUMER_QUEUE_1 = "Consumer_1";
    private static final String DIRECT_EXC_KEY = "Key_1";

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
        notConfirmedIds.clear();
        directChannel.queueDelete(CONSUMER_QUEUE_1);
        directChannel.exchangeDelete(EXCHANGE);

        directChannel.close();
        connection.close();
    }

    @Test
    public void should_publish_with_ids_respecting_auto_incremental_sequence() throws IOException, InterruptedException, TimeoutException {
        // Always define channel in "confirm mode", otherwise we won't be able to wait for confirms
        directChannel.confirmSelect();
        for (int i = 0; i < 10; i++) {
            directChannel.basicPublish("", CONSUMER_QUEUE_1, null, "Test".getBytes());
        }
        directChannel.waitForConfirms(5000);

        assertThat(directChannel.getNextPublishSeqNo()).isEqualTo(11L);
    }

    @Test
    public void should_confirm_all_messages_in_confirm_mode_channel() throws IOException, InterruptedException, TimeoutException {
        int msgCount = 10;
        directChannel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                notConfirmedIds.remove(deliveryTag);
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                // Message was lost, we just print the info for debug;
                // otherwise, this case should be handled differently
                System.out.println("Not-Acknowledging for message with id " + deliveryTag);
            }
        });

        directChannel.confirmSelect();
        // Publish messages to the channel and put the ids to the queue
        for (int i = 0; i < msgCount; i++) {
            // Add to set with not confirmed message ids
            notConfirmedIds.add(directChannel.getNextPublishSeqNo());
            directChannel.basicPublish("", CONSUMER_QUEUE_1, null, "Test".getBytes());
            directChannel.waitForConfirms(1000);
        }

        // Empty set proves that confirm listener is not linked
        // with messages (n)acknowledging by consumers - in this case
        // we only publish messages without consuming them.
        // Confirm listener is trigerred when RabbitMQ server successful
        // receives given message. In our case, successful receiving is
        // equal to push the message to the queue.
        assertThat(notConfirmedIds).isEmpty();
        int consumedMessages = 0;
        // The prof that messages haven't been consumed:
        while (directChannel.basicGet(CONSUMER_QUEUE_1, AUTO_ACK.yes()) != null) {
            consumedMessages++;
        }
        assertThat(consumedMessages).isEqualTo(10);
    }

    @Test
    public void should_not_confirm_all_messages_without_wait_for_confirm_blocking_call() throws IOException {
        directChannel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                notConfirmedIds.remove(deliveryTag);
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                // Message was lost, we just print the info for debug;
                // otherwise, this case should be handled differently
                System.out.println("Not-Acknowledging for message with id " + deliveryTag);
            }
        });

        directChannel.confirmSelect();
        // Publish messages to the channel and put the ids to the queue
        for (int i = 0; i < 10; i++) {
            // Add to set with not confirmed message ids
            notConfirmedIds.add(directChannel.getNextPublishSeqNo());
            directChannel.basicPublish("", CONSUMER_QUEUE_1, null, "Test".getBytes());
            // Note that we should call here waitForConfirms method
            // to be sure that next message is published always
            // after confirming the reception of previous one
            // directChannel.waitForConfirms(1000);
        }

        assertThat(notConfirmedIds).isNotEmpty();
    }

    @Test(expected = IllegalStateException.class)
    public void should_throw_an_exception_when_confirm_waiting_is_expected_for_channel_not_in_confirm_mode() throws TimeoutException, InterruptedException {
        directChannel.waitForConfirms(5000);
    }

}
