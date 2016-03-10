package com.waitingforcode.util;

import com.google.gson.Gson;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class RabbitMQApiHelper {

    private static final Client CLIENT = ClientBuilder.newClient();
    private static final Gson GSON = new Gson();

    public static List<Map<String, String>> query(String path) {
        WebTarget target = CLIENT.target("http://localhost:15672").path(path);
        String response = target.request(MediaType.APPLICATION_JSON_TYPE)
                .header("Authorization", "Basic " + Base64.getEncoder().encodeToString("guest:guest".getBytes())).get(String.class);
        return GSON.fromJson(response, ArrayList.class);
    }

}
