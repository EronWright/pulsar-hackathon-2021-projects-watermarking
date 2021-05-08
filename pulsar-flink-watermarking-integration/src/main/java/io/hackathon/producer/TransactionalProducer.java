package io.hackathon.producer;

import com.google.common.collect.Iterables;
import io.hackathon.config.AppConfig;
import io.hackathon.models.StationSensorReading;
import io.hackathon.utils.AdminUtils;
import io.hackathon.utils.AppUtils;
import io.hackathon.utils.ClientUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.policies.data.TenantInfo;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransactionalProducer {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, PulsarAdminException {
        PulsarAdmin pulsarAdmin = AdminUtils.initAdminClient();
        System.out.println(System.getProperty("user.dir"));
        List<StationSensorReading> stationSensorReadings = AppUtils.loadStationSensorReadingsData();

        // create tenant/namespace needed for the transactions
        HashSet<String> adminRoles = Stream.of("admin")
                .collect(Collectors.toCollection(HashSet::new));
        List<String> availableClusters = pulsarAdmin.clusters().getClusters();

        HashSet<String> allowedClusters = Stream.of(availableClusters.get(0))
                .collect(Collectors.toCollection(HashSet::new));

        TenantInfo tenantInfo = new TenantInfo(adminRoles, allowedClusters);

        AdminUtils.createTenant(pulsarAdmin, AppConfig.TRANSACTIONS_TENANT, tenantInfo);

        // create a namespace
        final String namespace = AppConfig.TRANSACTIONS_TENANT + "/" + AppConfig.TRANSACTIONS_NAMESPACE;
        System.out.println(namespace);
        AdminUtils.createNamespace(pulsarAdmin, namespace);

        // split the dataset into partitions of 1000 records each - 60 partitions
        Iterable<List<StationSensorReading>> partitions = Iterables.partition(stationSensorReadings, 1000);
        PulsarClient pulsarClient = ClientUtils.initPulsarClientWithTransactions();

        Producer<StationSensorReading> producer = ClientUtils.initTransactionalProducer(
                pulsarClient,
                AppConfig.topicNameSingle,
                "ssr-producer"
        );

        // 1. create a new transaction foreach partition
        // 2. send msg in the partition
        // 3. commit the transaction
        // 4. send a watermark
        int partitionNum = 0;
        for (List<StationSensorReading> partition : partitions) {
            // Create a transaction
            Transaction txn = pulsarClient
                    .newTransaction()
                    .withTransactionTimeout(5, TimeUnit.MINUTES)
                    .build()
                    .get();

            ClientUtils.doProducerWorkWithTransaction(producer, partition, txn);
            // commit at the end of each partition
            System.out.println("Transaction commit..");
            txn.commit()
                    .whenComplete((unused, throwable) -> {
                        if (throwable != null) {
                            System.out.println("Commit failed, aborting transaction");
                            try {
                                txn.abort().get();
                            } catch (InterruptedException | ExecutionException e) {
                                e.printStackTrace();
                            }
                        } else {
                            System.out.println("Transaction committed Successfully");
                        }
                    });
            partitionNum += 1;
            System.out.printf("Total partitions finished: %s%n", partitionNum);
            Thread.sleep(1000);
        }

        producer.close();
        pulsarClient.close();
        pulsarAdmin.close();
    }
}
