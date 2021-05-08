package io.hackathon.consumers;

import com.google.devtools.common.options.*;
import io.hackathon.config.*;

/**
 */
public class PulsarConsumerOptions extends OptionsBase {
    @Option(
            name = "fault-noack",
            help = "If enabled, the consumer doesn't ack any messages.",
            defaultValue = "false"
    )
    public boolean faultNoack;

    @Option(
            name = "topic",
            help = "Topic name.",
            defaultValue = AppConfig.topicNameSingle
    )
    public String topicName;

    @Option(
            name = "subscription",
            help = "Subscription name.",
            defaultValue = "watermarking-subs-test"
    )
    public String subscriptionName;

    @Option(
            name = "consumer",
            help = "Consumer name.",
            defaultValue = "watermarking-consumer"
    )
    public String consumerName;
}
