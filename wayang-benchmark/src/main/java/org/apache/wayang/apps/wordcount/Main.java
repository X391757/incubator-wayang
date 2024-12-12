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

package org.apache.wayang.apps.wordcount;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class Main {

    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length < 2) {
            System.err.println("Usage: <platform1>[,<platform2>]* <input parquet file>");
            System.exit(1);
        }

        WayangContext wayangContext = new WayangContext();
        for (String platform : args[0].split(",")) {
            switch (platform) {
                case "java":
                    wayangContext.register(Java.basicPlugin());
                    break;
                case "spark":
                    wayangContext.register(Spark.basicPlugin());
                    break;
                default:
                    System.err.format("Unknown platform: \"%s\"\n", platform);
                    System.exit(3);
                    return;
            }
        }
        long stime = System.currentTimeMillis();
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Parquet Analysis")
                .withUdfJarOf(Main.class);
        Collection<Tuple2<String, Integer>> valueCounts = planBuilder
                .readParquetFile(args[1]) // Assuming a method that supports reading directly into GenericRecords
                .withName("Load Parquet file")

                .map(record -> {
                    Object columnValue = record.get("DEP_TIME"); // Access by column name
                    return columnValue != null ? columnValue.toString() : "null";
                })
                .withName("Extract target column")

                .filter(value -> !value.equals("null"))
                .withName("Filter null values")

                .map(value -> new Tuple2<>(value, 1))
                .withName("Add counter")

                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withName("Count values")

                .collect();
        long etime = System.currentTimeMillis();
        System.out.printf("Found %d distinct values:\n", valueCounts.size());
        valueCounts.forEach(count -> System.out.printf("%dx %s\n", count.field1, count.field0));
        System.out.printf("runtime %d ms.", (etime - stime));
    }
}

