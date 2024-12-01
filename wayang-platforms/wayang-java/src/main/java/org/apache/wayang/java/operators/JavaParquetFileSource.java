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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
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
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.proto.ProtoParquetReader;
import org.apache.parquet.proto.ProtoReadSupport;
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
 * This is the platform-specific execution operator that implements the {@link ParquetFileSource}.
 */
public class JavaParquetFileSource extends ParquetFileSource implements JavaExecutionOperator {

    private static final Logger logger = LoggerFactory.getLogger(JavaParquetFileSource.class);

    public JavaParquetFileSource(String inputUrl, Descriptor descriptor) {
        super(inputUrl, descriptor);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that the instance to copy.
     */
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

        try {
            // Configure Protobuf Descriptor in the Hadoop Configuration
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            conf.set(ProtoReadSupport.PB_CLASS, this.getDescriptor().getFullName());

            // Create a ProtoParquetReader
            try (ParquetReader<DynamicMessage> reader = ProtoParquetReader.<DynamicMessage>builder(new Path(inputUrl))
                    .withConf(conf)
                    .build()) {

                Stream<DynamicMessage> messageStream = Stream.generate(() -> {
                    try {
                        return reader.read();
                    } catch (IOException e) {
                        throw new WayangException(String.format("Error reading Parquet file at %s.", inputUrl), e);
                    }
                }).takeWhile(message -> message != null);

                // Pass the stream to the output channel
                ((StreamChannel.Instance) outputs[0]).accept(messageStream);

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
        return new JavaParquetFileSource(this.getInputUrl(), this.getDescriptor());
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
