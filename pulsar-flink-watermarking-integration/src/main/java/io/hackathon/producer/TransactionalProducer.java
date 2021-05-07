package io.hackathon.producer;

import com.google.common.collect.Iterables;
import io.hackathon.config.AppConfig;
import io.hackathon.models.StationSensorReading;
import io.hackathon.utils.AppUtils;
import io.hackathon.utils.ClientUtils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class TransactionalProducer {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        List<StationSensorReading> stationSensorReadings = AppUtils.loadStationSensorReadingsData();

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
            Thread.sleep(2000);
        }

        producer.close();
        pulsarClient.close();
    }
}
