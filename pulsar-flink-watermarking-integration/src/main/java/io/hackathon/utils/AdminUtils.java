package io.hackathon.utils;

import io.hackathon.config.AppConfig;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;

public class AdminUtils {

    public static PulsarAdmin initAdminClient() throws PulsarClientException {
        return PulsarAdmin
                .builder()
                .serviceHttpUrl(AppConfig.ADMIN_URL)
                .build();
    }

    public static void createPartitionedTopics(final PulsarAdmin pulsarAdmin,
                                               String topicName,
                                               int numberOfPartitions) {
        try {
            pulsarAdmin.topics().createPartitionedTopic(topicName, numberOfPartitions);
            System.out.printf("Created topic '%s' with %d partitions%n", topicName, numberOfPartitions);
        } catch (PulsarAdminException.ConflictException conflictException) {
            System.out.printf("Partitioned topic '%s' already exists%n", topicName);
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
    }
}
