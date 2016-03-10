package com.waitingforcode;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public abstract class BaseConfig {

    public static final ConnectionFactory CONNECTION_FACTORY = new ConnectionFactory();
    private static final String HOST = "localhost";
    private static final String ADMIN_LOGIN = "bartosz";
    private static final String USERS_PASSWORD = "very_secret_password";
    static {
        CONNECTION_FACTORY.setHost(HOST);
        CONNECTION_FACTORY.setUsername(ADMIN_LOGIN);
        CONNECTION_FACTORY.setPassword(USERS_PASSWORD);
    }

    protected static Connection getConnection() throws IOException, TimeoutException {
        return CONNECTION_FACTORY.newConnection();
    }

}
