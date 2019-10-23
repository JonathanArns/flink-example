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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.sql.*;
import java.util.Properties;


/**
 * Flink Streaming Job to generate primary keys for json messages in kafka
 *
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		String jobName = parameterTool.get("job-name", "DatabaseHashingJob");
		String postgresUrl = parameterTool.get("postgres-url", "jdbc:postgresql://postgres:5432/kafka_connect");
		String postgresUser = parameterTool.get("postgres-user", "kafka_connect");
		String postgresPassword = parameterTool.get("postgres-password", "kafka_connect");

		Connection dbc = DriverManager.getConnection(postgresUrl, postgresUser, postgresPassword);
		Statement stmt = dbc.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test_topic");
		rs.next();
		int count = rs.getInt(1);
		throw new Exception("Count DB Elements: "+count);
	}
}
