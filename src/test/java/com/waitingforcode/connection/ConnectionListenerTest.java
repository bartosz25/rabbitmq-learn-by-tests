package com.waitingforcode.connection;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import com.waitingforcode.BaseConfig;
import com.waitingforcode.util.ExchangeTypes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.waitingforcode.util.Mapping.BoolMaps.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * As {@link RecoveryTest}, these tests also need to be executed with manual restart of RabbitMQ server.
 *
 * @see RecoveryTest
 */
public class ConnectionListenerTest extends BaseConfig {

    private static final String NOTIF_SHUTDOWN = "shutdown";
    private static final String NOTIF_RECOVERY = "recovery";
    private static final String NOTIF_QUEUE_RECOVERY = "queue_recovery";

    private List<String> notificationStats = new ArrayList<>();

    @BeforeClass
    public static void initPersistence() {
        CONNECTION_FACTORY.setAutomaticRecoveryEnabled(true);
        CONNECTION_FACTORY.setNetworkRecoveryInterval(250);
    }

    @After
    public void resetStats() {
        notificationStats.clear();
    }

    @AfterClass
    public static void resetPersistence() {
        CONNECTION_FACTORY.setAutomaticRecoveryEnabled(false);
    }

    @Test
    public void should_notify_shutdown_listener_about_server_shutdown() throws IOException, TimeoutException, InterruptedException {
        AutorecoveringConnection localConnection = (AutorecoveringConnection) CONNECTION_FACTORY.newConnection();
        try {
            localConnection.addShutdownListener((cause) -> {
                    System.out.println("Shutdown completed because of protocol method: "+cause.getReason().protocolMethodName());
                    notificationStats.add(NOTIF_SHUTDOWN);
                }
            );
            System.out.println("Please restart a server now");
            Thread.sleep(10_000);

        } finally {
            localConnection.close();
        }

        assertThat(notificationStats).contains(NOTIF_SHUTDOWN);
    }

    @Test
    public void should_notify_shutdown_listener_about_server_recovery() throws IOException, InterruptedException, TimeoutException {
        AutorecoveringConnection localConnection = (AutorecoveringConnection) CONNECTION_FACTORY.newConnection();
        try {
            localConnection.addRecoveryListener((recoverable) -> {
                    System.out.println("Trying to recover connection");
                    notificationStats.add(NOTIF_RECOVERY);
                }
            );
            System.out.println("Please restart a server now");
            Thread.sleep(10_000);

        } finally {
            localConnection.close();
        }

        assertThat(notificationStats).contains(NOTIF_RECOVERY);
    }

    @Test
    public void should_notify_shutdown_listener_about_queue_recovery() throws IOException, InterruptedException, TimeoutException {
        String[] oldNewName = new String[2];
        String exchangeName = "tmp_exchange";
        String queueName = "tmp_queue";
        AutorecoveringConnection localConnection = (AutorecoveringConnection) CONNECTION_FACTORY.newConnection();
        Channel directChannel = localConnection.createChannel();
        directChannel.exchangeDeclare(exchangeName, ExchangeTypes.DIRECT.getName());
        directChannel.queueDeclare(queueName, DURABLE.not(), EXCLUSIVE.not(), AUTO_DELETE.yes(),
                Collections.emptyMap());
        directChannel.queueBind(queueName, exchangeName, "key");
        try {
            localConnection.addQueueRecoveryListener((oldName, newName) -> {
                oldNewName[0] = oldName;
                oldNewName[1] = newName;
                notificationStats.add(NOTIF_QUEUE_RECOVERY);
            });
            System.out.println("Please restart a server now");
            Thread.sleep(10_000);

        } finally {
            directChannel.exchangeDelete(exchangeName);
            localConnection.close();
        }

        assertThat(notificationStats).contains(NOTIF_QUEUE_RECOVERY);
        assertThat(oldNewName[0]).isEqualTo(queueName);
        assertThat(oldNewName[1]).isEqualTo(queueName);
    }

}
