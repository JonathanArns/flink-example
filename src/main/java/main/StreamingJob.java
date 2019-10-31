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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;

import java.util.*;


/**
 * Flink Streaming Job to generate primary keys for json messages in kafka
 *
 */
public class StreamingJob {
    private static String postgresHost, postgresDB, postgresUser, postgresPassword;
    private static String inputTable = "input_table";

    public static final String ES_SECURITY_ENABLE = "es.security.enable";
    public static final String ES_SECURITY_USERNAME = "es.security.username";
    public static final String ES_SECURITY_PASSWORD = "es.security.password";

	public static void main(String[] args) throws Exception {
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		String jobName = parameterTool.get("job-name", "DatabaseHashingJob");
		postgresHost = parameterTool.get("postgres-host", "postgres:5432");
        postgresDB = parameterTool.get("postgres-db", "flink_db");
		postgresUser = parameterTool.get("postgres-user", "postgres");
		postgresPassword = parameterTool.get("postgres-password", "postgres");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Row> inputData = env.createInput(StreamingJob.createJDBCSource());
        inputData.print();
        DataStream<String[]> inputDataString = inputData.map(new MapFunction<Row, String[]>() {
            @Override
            public String[] map(Row value) throws Exception {

                return new String[] {value.getField(0).toString(), value.getField(1).toString(), value.getField(2).toString()};
            }
        });
        //SingleOutputStreamOperator<Row> transformedSet = inputData.filter(row -> Integer.parseInt(row.getField(0).toString()) < 3);
        //transformedSet.print();
        //transformedSet.writeUsingOutputFormat(StreamingJob.createElasticsearchSink());
        inputDataString.print();
        inputDataString.addSink(createElasticsearchSink().build());
        env.execute();
	}

    private static JDBCInputFormat createJDBCSource() {

        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO };

        return JDBCInputFormat.buildJDBCInputFormat()
                .setDBUrl(String.format("jdbc:postgresql://%s/%s", StreamingJob.postgresHost, StreamingJob.postgresDB))
                .setDrivername("org.postgresql.Driver")
                .setUsername(StreamingJob.postgresUser)
                .setPassword(StreamingJob.postgresPassword)
                .setQuery("SELECT id, name ,location FROM "+StreamingJob.inputTable)
                .setRowTypeInfo(new RowTypeInfo(fieldTypes))
                .finish();
    }

    private static ElasticsearchSink.Builder<String[]> createElasticsearchSink() throws Exception{

        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "docker-cluster");
// This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");
        // config.put();

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("elasticsearch", 9200, "http"));

        ElasticsearchSink.Builder<String[]> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String[]>() {
                    public IndexRequest createIndexRequest(String[] element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("id", element[0]);
                        json.put("name", element[1]);
                        json.put("location", element[2]);

                        return Requests.indexRequest()
                                .index("example-index")
                                .type("example-type")
                                .source(json);
                    }

                    @Override
                    public void process(String[] element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esSinkBuilder.setBulkFlushMaxActions(1);
        // provide a RestClientFactory for custom configuration on the internally created REST client
        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                            // elasticsearch username and password
                            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "BRHhwoXneUaV8fYvLcu5"));
                            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    });
                }
        );

        return esSinkBuilder;

    }
}
