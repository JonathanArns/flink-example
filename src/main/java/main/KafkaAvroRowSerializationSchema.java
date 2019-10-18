package main;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.AvroRowSerializationSchema;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class KafkaAvroRowSerializationSchema extends AvroRowSerializationSchema implements KafkaSerializationSchema<Row> {


    public KafkaAvroRowSerializationSchema(String avroSchemaString) {
        super(avroSchemaString);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Row element, @Nullable Long timestamp) {
        return new ProducerRecord<byte[], byte[]>("test_topic", null, super.serialize(element));
    }
}
