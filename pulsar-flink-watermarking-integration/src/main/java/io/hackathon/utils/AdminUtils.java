package io.hackathon.utils;

import io.hackathon.config.AppConfig;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.TenantInfo;

public class AdminUtils {

    public static PulsarAdmin initAdminClient() throws PulsarClientException {
        return PulsarAdmin
                .builder()
                .serviceHttpUrl(AppConfig.ADMIN_URL)
                .build();
    }

    public static void createTenant(final PulsarAdmin pulsarAdmin, String tenantName, TenantInfo tenantInfo) {
        try {
            pulsarAdmin.tenants().createTenant(tenantName, tenantInfo);
            System.out.printf("Created tenant '%s'%n", tenantName);
        } catch (PulsarAdminException.ConflictException conflictException) {
            System.out.printf("Tenant '%s' already exists%n", tenantName);
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
    }

    public static void createNamespace(final PulsarAdmin pulsarAdmin, final String namespace) {
        try {
            pulsarAdmin.namespaces().createNamespace(namespace);
            System.out.printf("Created namespace '%s'%n", namespace);
        } catch (PulsarAdminException.ConflictException conflictException) {
            System.out.printf("Namespace '%s' already exists%n", namespace);
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
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
