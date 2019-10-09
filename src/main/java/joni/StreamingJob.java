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

package joni;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

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
		capitalize();
	}

	public static void capitalize() throws Exception {
		String inputTopic = "flink_input";
		String outputTopic = "flink_output";
		String consumerGroup = "wordCapitalizer";
		String address = "kafka:29092"; // for running in eclipse use "localhost:9092"
		StreamExecutionEnvironment environment = StreamExecutionEnvironment
				.getExecutionEnvironment();
		FlinkKafkaConsumer<String> flinkKafkaConsumer = Helper.createStringConsumerForTopic(
				inputTopic, address, consumerGroup);
		DataStream<String> stringInputStream = environment
				.addSource(flinkKafkaConsumer);

		FlinkKafkaProducer<String> flinkKafkaProducer = Helper.createStringProducer(
				outputTopic, address);

		stringInputStream
				.map(new WordsCapitalizer())
				.addSink(flinkKafkaProducer);

		environment.execute("Word capitalization");
	}
}
