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
package org.apache.wayang.java.operators;
import org.apache.hadoop.conf.Configuration;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.wayang.basic.operators.ParquetFileSource;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.core.util.Tuple;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * This is the platform-specific execution operator that implements the {@link ParquetFileSource} for Avro format.
 */
public class JavaParquetFileSource extends ParquetFileSource implements JavaExecutionOperator {

    private static final Logger logger = LoggerFactory.getLogger(JavaParquetFileSource.class);

    public JavaParquetFileSource(String inputUrl) {
        super(inputUrl);
    }
    
    public JavaParquetFileSource(ParquetFileSource that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        String inputUrl = this.getInputUrl();
        Path path = new Path(inputUrl);
        try {
            InputFile inputFile = HadoopInputFile.fromPath(path, new Configuration());
            // Create an AvroParquetReader
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile)
                    .build()) {
                Stream<GenericRecord> recordStream = Stream.generate(() -> {
                    try {
                        return reader.read();
                    } catch (IOException e) {
                        throw new RuntimeException("Error closing ParquetReader", e);
                    }
                })
                .takeWhile(record -> record != null) // Stop the stream when no more records are found
                .onClose(() -> {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        throw new RuntimeException("Error closing ParquetReader", e);
                    }
                });

                // Pass the stream to the output channel
                ((StreamChannel.Instance) outputs[0]).accept(recordStream);

            } catch (IOException e) {
                throw new WayangException(String.format("Failed to read from Parquet file at %s.", inputUrl), e);
            }

        } catch (Exception e) {
            throw new WayangException(String.format("Error accessing Parquet file at %s.", inputUrl), e);
        }

        // Create lineage nodes for the evaluation
        ExecutionLineageNode prepareLineageNode = new ExecutionLineageNode(operatorContext);
        prepareLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.parquetfilesource.load.prepare", javaExecutor.getConfiguration()
        ));
        ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);
        mainLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.parquetfilesource.load.main", javaExecutor.getConfiguration()
        ));

        outputs[0].getLineage().addPredecessor(mainLineageNode);

        return prepareLineageNode.collectAndMark();
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("wayang.java.parquetfilesource.load.prepare", "wayang.java.parquetfilesource.load.main");
    }

    @Override
    public JavaParquetFileSource copy() {
        return new JavaParquetFileSource(this.getInputUrl());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }
}

