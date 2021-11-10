package org.felixzh.kafka.admin_api;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;

public class KafkaAdminAPI {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "felixzh2:9092");
        AdminClient adminClient = AdminClient.create(properties);
        adminClient.close();
    }
}
