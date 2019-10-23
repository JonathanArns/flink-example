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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.stream.Stream;


/**
 * Flink Streaming Job to generate primary keys for json messages in kafka
 *
 */
public class StreamingJob {
    private static String postgresHost, postgresDB, postgresUser, postgresPassword;
    private static String inputTable = "input_table", outputTable = "output_table";
	public static void main(String[] args) throws Exception {
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		String jobName = parameterTool.get("job-name", "DatabaseHashingJob");
		postgresHost = parameterTool.get("postgres-host", "postgres:5432");
        postgresDB = parameterTool.get("postgres-db", "flink_db");
		postgresUser = parameterTool.get("postgres-user", "postgres");
		postgresPassword = parameterTool.get("postgres-password", "postgres");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Row> inputData = env.createInput(StreamingJob.createJDBCSource());
        inputData.print();
        SingleOutputStreamOperator<Row> transformedSet = inputData.filter(row -> Integer.parseInt(row.getField(0).toString()) < 3);
        transformedSet.print();
        transformedSet.writeUsingOutputFormat(StreamingJob.createJDBCSink());

        env.execute();
	}



    private static JDBCOutputFormat createJDBCSink() {
        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO };

        return JDBCOutputFormat.buildJDBCOutputFormat()
                .setDBUrl(String.format("jdbc:postgresql://%s/%s", StreamingJob.postgresHost, StreamingJob.postgresDB))
                .setDrivername("org.postgresql.Driver")
                .setUsername(StreamingJob.postgresUser)
                .setPassword(StreamingJob.postgresPassword)
                .setQuery(String.format("insert into %s (id, name, location) values (?,?,?)", StreamingJob.outputTable))
                .setSqlTypes(new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR})
                .setBatchInterval(200)
                .finish();
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
}
