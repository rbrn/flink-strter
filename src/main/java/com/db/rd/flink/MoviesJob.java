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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import scala.Tuple2;

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
public class MoviesJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple3<Long, String, String>> dataSource = env.readCsvFile("/home/costin/Downloads/ml-latest-small/movies.csv")
				.ignoreFirstLine()
				.parseQuotedStrings('"')
				.ignoreInvalidLines()
				.types(Long.class, String.class, String.class);

		DataSet<Movies> moviesDataSet = dataSource.map(new MapFunction<Tuple3<Long, String, String>, Movies>() {
			@Override
			public Movies map(Tuple3<Long, String, String> longStringStringTuple3) throws Exception {
				String movie = longStringStringTuple3.f1;
				String [] genres = longStringStringTuple3.f2.split("\\|");
				return new Movies(movie, new HashSet<>(Arrays.asList(genres)));
			}
		});

		moviesDataSet.filter(new FilterFunction<Movies>() {
			@Override
			public boolean filter(Movies movies) throws Exception {
				return movies.getGenres() != null && movies.getGenres().contains("Drama");
			}
		});

		moviesDataSet.writeAsText("filtered-output");

		env.execute();

	}


}

class Movies{
	private String name;

	public Movies(String name, Set<String> tags){
		this.name = name;
		this.genres = tags;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Set<String> getGenres() {
		return genres;
	}

	public void setGenres(Set<String> genres) {
		this.genres = genres;
	}

	private Set<String> genres;

	@Override
	public String toString() {
		return String.format("Movies{name=%s, genres=%s}", name, genres);
	}
}