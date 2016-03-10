package com.waitingforcode.connection;

import com.rabbitmq.client.Connection;
import com.waitingforcode.BaseConfig;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test cases for automatic recovery. After the launching on each tests, please restart RabbitMQ server
 * (for example with: sudo service rabbitmq-server restart) when a message "Please restart a server now" appears.
 * Manual restarts are the most universal way to check recovery feature (otherwise, you could for example allow executing
 * server restart without giving sudo password and do it from Java code, but it's not the subject here)
 */
public class RecoveryTest extends BaseConfig {

    @AfterClass
    public static void resetRecovery() {
        CONNECTION_FACTORY.setAutomaticRecoveryEnabled(false);
    }

    @Test
    public void should_recover_automatically_after_server_restart() throws IOException, TimeoutException, InterruptedException {
        CONNECTION_FACTORY.setAutomaticRecoveryEnabled(true);
        Connection connection = CONNECTION_FACTORY.newConnection();
        System.out.println("Please restart a server now");
        Thread.sleep(10000);

        assertThat(connection.isOpen()).isTrue();
    }

    @Test
    public void should_not_recover_automatically_after_server_restart_with_recovery_turned_off() throws IOException, TimeoutException, InterruptedException {
        CONNECTION_FACTORY.setAutomaticRecoveryEnabled(false);
        Connection connection = CONNECTION_FACTORY.newConnection();
        System.out.println("Please restart a server now");
        Thread.sleep(10000);

        assertThat(connection.isOpen()).isFalse();
    }

}
