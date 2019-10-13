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

package com.db.rd.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class AverageRatingJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple3<Long, String, String>> movies = env.readCsvFile("/home/costin/Downloads/ml-latest-small/movies.csv")
				.ignoreFirstLine()
				.parseQuotedStrings('"')
				.ignoreInvalidLines()
				.types(Long.class, String.class, String.class);

		DataSource<Tuple2<Long, Double>> ratings = env.readCsvFile("/home/costin/Downloads/ml-latest-small/ratings.csv")
				.ignoreFirstLine()
				.includeFields(false, true, true, false)
				.types(Long.class, Double.class);


		DataSet aggregates = movies.join(ratings)
				.where(0)
				.equalTo(0)
				.with(new JoinFunction<Tuple3<Long, String, String>, Tuple2<Long, Double>, Tuple3<String, String, Double>>() {
					@Override
					public Tuple3<String, String, Double> join(Tuple3<Long, String, String> movie, Tuple2<Long, Double> rating) throws Exception {
						String name = movie.f1;
						String genre = movie.f2.split("\\|")[0];
						Double score = rating.f1;
						return new Tuple3<>(name, genre, score);
					}
				})
				.groupBy(1)
				.reduceGroup(new GroupReduceFunction<Tuple3<String, String, Double>, Object>() {
					@Override
					public void reduce(Iterable<Tuple3<String, String, Double>> iterable, Collector<Object> collector) throws Exception {
						String genre = null;
						int count = 0;
						double totalScore = 0;

						for (Tuple3<String, String, Double> movie : iterable){
							genre = movie.f1;
							totalScore += movie.f2;
							count++;
						}

						collector.collect(new Tuple2< >(genre, totalScore/ count));
					}
				});

		aggregates.print();
		env.execute();

	}


}