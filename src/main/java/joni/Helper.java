package joni;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.util.Properties;

public class Helper {
    public static FlinkKafkaConsumer<String> createStringConsumerForTopic(
            String topic, String kafkaAddress, String kafkaGroup ) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        props.setProperty("group.id",kafkaGroup);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), props);

        return consumer;
    }

    public static FlinkKafkaProducer<String> createStringProducer(
            String topic, String kafkaAddress){

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        return new FlinkKafkaProducer<String>(
                topic,
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                props,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
    }
}
