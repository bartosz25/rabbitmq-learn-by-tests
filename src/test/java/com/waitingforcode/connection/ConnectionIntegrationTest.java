package com.waitingforcode.connection;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;

/**
 * Tests for RabbitMQ connections. Before launching this tests, launch these commands:
 * <pre>
 * sudo rabbitmqctl add_user bartosz very_secret_password
 * Creating user "bartosz" ...
 * ...done.
 * </pre>
 *
 * <pre>
 *  sudo rabbitmqctl set_permissions -p / bartosz ".*" ".*" ".*"
 *  Setting permissions for user "bartosz" in vhost "/" ...
 *  ...done.
 * </pre>
 *
 * <pre>
 *  sudo rabbitmqctl set_user_tags bartosz Admin
 *  Setting tags for user "bartosz" to ['Admin'] ...
 *  ...done
 *  </pre>
 *
 * It should add user called bartosz to RabbitMQ and give him a full access configure,
 * read and write) to / vhost. In additionally, bartosz user should be an administrator, ie.
 * he could be able to manage vhosts, users, permissions and connections. After, let's make
 * the same thing but for user having permission only to read:
 * <pre>
 * sudo rabbitmqctl add_user bartosz_reader very_secret_password
 * sudo rabbitmqctl set_permissions -p / bartosz_reader "" "" ".*"
 * </pre>
 */
public class ConnectionIntegrationTest {

    private static final String HOST = "localhost";
    private static final String ADMIN_LOGIN = "bartosz";
    private static final String MODERATOR_LOGIN = "bartosz_reader";
    private static final String USERS_PASSWORD = "very_secret_password";

    @Test(expected =  UnknownHostException.class)
    public void should_not_connect_to_bad_host() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("inactive_host");
        factory.newConnection();
    }

    @Test(expected = IOException.class)
    public void should_not_be_able_to_create_exchange_without_config_rights() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setUsername(MODERATOR_LOGIN);
        factory.setPassword(USERS_PASSWORD);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare("exchange_name", "direct", true);
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, "exchange_name", "routing_key");

        channel.close();
        connection.close();
    }

    @Test
    public void should_correctly_declare_a_queue_because_of_config_rights() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setUsername(ADMIN_LOGIN);
        factory.setPassword(USERS_PASSWORD);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        String exchangeName = "exchange_name";
        channel.exchangeDeclare(exchangeName, "direct", true);

        // check if exchange was correctly created
        channel.exchangeDeclarePassive(exchangeName);

        channel.exchangeDelete(exchangeName);
        channel.close();
        connection.close();
    }


}