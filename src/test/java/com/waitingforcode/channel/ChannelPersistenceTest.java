package com.waitingforcode.channel;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import com.waitingforcode.BaseConfig;
import com.waitingforcode.util.ExchangeTypes;
import com.waitingforcode.util.CountingConsumer;
import com.waitingforcode.util.Mapping;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.waitingforcode.util.Mapping.BoolMaps.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Also needs manual server restart.
 *
 * @see com.waitingforcode.connection.RecoveryTest
 */
public class ChannelPersistenceTest extends BaseConfig {

    private static final String EXCHANGE_KEY = "Persistence_Exchange";
    private static final String QUEUE_NAME = "Persistence_Test_Queue";

    @BeforeClass
    public static void initPersistence() {
        CONNECTION_FACTORY.setAutomaticRecoveryEnabled(true);
        CONNECTION_FACTORY.setNetworkRecoveryInterval(500);
    }

    @AfterClass
    public static void resetPersistence() {
        CONNECTION_FACTORY.setAutomaticRecoveryEnabled(false);
    }

    @Test
    public void should_not_deliver_a_persistent_message_sent_to_not_durable_queue() throws IOException, InterruptedException, TimeoutException {
        AutorecoveringConnection localConnection = (AutorecoveringConnection) CONNECTION_FACTORY.newConnection();
        AutorecoveringChannel newChannel = (AutorecoveringChannel) localConnection.createChannel();
        try {
            newChannel.exchangeDeclare(EXCHANGE_KEY, ExchangeTypes.DIRECT.getName());
            // queue must be durable, otherwise the messages won't be stored after crash even if they are marked "persistent"
            newChannel.queueDeclare(QUEUE_NAME, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(), Collections.emptyMap());
            newChannel.queueBind(QUEUE_NAME, EXCHANGE_KEY, "");

            CountDownLatch latch = new CountDownLatch(1);
            AMQP.BasicProperties properties = new AMQP.BasicProperties().builder()
                    .deliveryMode(Mapping.DeliveryModes.PERSISTENT.value()).build();
            String message = "Test_Not_Persistent";
            newChannel.basicPublish(EXCHANGE_KEY, "", properties, message.getBytes());

            System.out.println("Please restart a server now");
            Thread.sleep(10_000);

            Map<String, Integer> consumed = new HashMap<>();
            newChannel.basicConsume(QUEUE_NAME, AUTO_ACK.yes(), new CountingConsumer(newChannel, latch, consumed));

            latch.await(2, TimeUnit.SECONDS);

            assertThat(newChannel.isOpen()).isTrue();
            assertThat(consumed).isEmpty();
        } finally {
            newChannel.queueDelete(QUEUE_NAME);
            newChannel.exchangeDelete(EXCHANGE_KEY);

            newChannel.close();
            localConnection.close();
        }
    }

    @Test
    public void should_deliver_again_persistent_message_after_server_restart() throws IOException, InterruptedException, TimeoutException {
        AutorecoveringConnection localConnection = (AutorecoveringConnection) CONNECTION_FACTORY.newConnection();
        AutorecoveringChannel newChannel = (AutorecoveringChannel) localConnection.createChannel();
        try {
            newChannel.exchangeDeclare(EXCHANGE_KEY, ExchangeTypes.DIRECT.getName());
            // queue must be durable, otherwise the messages won't be stored after crash even if they are marked "persistent"
            newChannel.queueDeclare(QUEUE_NAME, DURABLE.yes(), EXCLUSIVE.not(), AUTO_DELETE.yes(), Collections.emptyMap());
            newChannel.queueBind(QUEUE_NAME, EXCHANGE_KEY, "");

            CountDownLatch latch = new CountDownLatch(1);
            AMQP.BasicProperties properties = new AMQP.BasicProperties().builder()
                    .deliveryMode(Mapping.DeliveryModes.PERSISTENT.value()).build();
            String message = "Test_Not_Persistent";
            newChannel.basicPublish(EXCHANGE_KEY, "", properties, message.getBytes());

            System.out.println("Please restart a server now");
            Thread.sleep(10_000);

            Map<String, Integer> consumed = new HashMap<>();
            newChannel.basicConsume(QUEUE_NAME, AUTO_ACK.yes(), new CountingConsumer(newChannel, latch, consumed));

            latch.await(2, TimeUnit.SECONDS);

            assertThat(newChannel.isOpen()).isTrue();
            assertThat(consumed.get(message).intValue()).isEqualTo(1);
        } finally {
            newChannel.queueDelete(QUEUE_NAME);
            newChannel.exchangeDelete(EXCHANGE_KEY);

            newChannel.close();
            localConnection.close();
        }
    }

    @Test
    public void should_not_deliver_again_not_persistent_message_after_first_failure() throws IOException, InterruptedException, TimeoutException {
        AutorecoveringConnection localConnection = (AutorecoveringConnection) CONNECTION_FACTORY.newConnection();
        AutorecoveringChannel newChannel = (AutorecoveringChannel) localConnection.createChannel();
        try {
            newChannel.exchangeDeclare(EXCHANGE_KEY, ExchangeTypes.DIRECT.getName());
            // queue must be durable, otherwise the messages won't be stored after crash even if they are marked "persistent"
            newChannel.queueDeclare(QUEUE_NAME, DURABLE.yes(), EXCLUSIVE.not(), AUTO_DELETE.yes(), Collections.emptyMap());
            newChannel.queueBind(QUEUE_NAME, EXCHANGE_KEY, "");

            CountDownLatch latch = new CountDownLatch(1);

            AMQP.BasicProperties properties = new AMQP.BasicProperties().builder()
                    .deliveryMode(Mapping.DeliveryModes.NOT_PERSISTENT.value()).build();
            String message = "Test_Not_Persistent";
            newChannel.basicPublish("exchange", "", properties, message.getBytes());

            System.out.println("Please restart a server now");
            Thread.sleep(10_000);

            Map<String, Integer> consumed = new HashMap<>();
            newChannel.basicConsume(QUEUE_NAME, AUTO_ACK.yes(), new CountingConsumer(newChannel, latch, consumed));

            latch.await(2, TimeUnit.SECONDS);

            assertThat(newChannel.isOpen()).isTrue();
            assertThat(consumed).isEmpty();
        } finally {
            newChannel.queueDelete(QUEUE_NAME);
            newChannel.exchangeDelete(EXCHANGE_KEY);

            newChannel.close();
            localConnection.close();
        }
    }
}
