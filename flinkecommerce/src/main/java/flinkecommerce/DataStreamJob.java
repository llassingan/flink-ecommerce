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

package flinkecommerce;

import Deserializer.JSONValueDeserializationSchema;
import Dto.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStreamJob {
	private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String topic = "financial_trx";
		LOG.info("Starting Flink job to consume from topic: {}", topic);

		KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
				.setBootstrapServers("broker:29092")
				.setTopics(topic)
				.setGroupId("flink-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new JSONValueDeserializationSchema())
				.build();

		DataStream<Transaction> transactionStream = env.fromSource(source, WatermarkStrategy.noWatermarks(),"Kafka source");

		transactionStream
				.map(transaction -> {
					LOG.info("Received transaction: {}", transaction);
					return transaction;
				})
				.print();

		LOG.info("Starting Flink execution");
		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}
}
