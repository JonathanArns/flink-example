/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		String jobName = "example_job";
		String inputTopic = "test_topic";
		String outputTopic = "test_topic2";
		String consumerGroup = "groupID";
		String kafkaAddress = "localhost:9092"; // for running in eclipse use "localhost:9092", for flink cluster "kafka:29092"
		//get the execution environment
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

		//create a new kafka consumer -> this is where your data comes from
		Properties consumerProps = new Properties();
		consumerProps.setProperty("bootstrap.servers", kafkaAddress);
		consumerProps.setProperty("group.id",consumerGroup);
		FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(inputTopic,
				new SimpleStringSchema(), consumerProps);

		//add the consumer to the environment as a data-source, to get a DataStream
		DataStream<String> dataStream = environment.addSource(flinkKafkaConsumer);

		//parse the json messages
		DataStream<ObjectNode> jsonStream = dataStream.map(new MapFunction<String,ObjectNode>() {
			ObjectMapper objectMapper = new ObjectMapper();
			@Override
			public ObjectNode map(String value) throws JsonProcessingException {
				return (ObjectNode)objectMapper.readTree(value);
			}
		}).map(new MapFunction<ObjectNode, ObjectNode>() {
			ObjectMapper objectMapper = new ObjectMapper();
			@Override
			public ObjectNode map(ObjectNode value) throws Exception {
				ObjectNode tmp = (ObjectNode)value.get("payload");
				tmp.put("first_name", tmp.get("first_name").textValue().toUpperCase());
				return value;
			}
		});

		//TODO: your calculations

		DataStream<String> outputStream = jsonStream.map(new MapFunction<ObjectNode, String>() {
			ObjectMapper objectMapper = new ObjectMapper();
			@Override
			public String map(ObjectNode value) throws Exception {
				return objectMapper.writeValueAsString(value);
			}
		});

		//create a new kafka producer -> this is where your results will go
		Properties producerProps = new Properties();
		producerProps.setProperty("bootstrap.servers", kafkaAddress);
		FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>(outputTopic,
				new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
				producerProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

		//add the producer to the dataStream as a sink
		outputStream.addSink(flinkKafkaProducer);

		environment.execute(jobName);
	}
}
