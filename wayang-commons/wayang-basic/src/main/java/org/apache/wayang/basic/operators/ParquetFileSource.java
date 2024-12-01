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
package org.apache.wayang.basic.operators;

import com.google.protobuf.Descriptors.Descriptor;
import org.apache.parquet.hadoop.ParquetReader;
import com.google.protobuf.DynamicMessage;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.proto.ProtoParquetReader;
import org.apache.parquet.proto.ProtoReadSupport;
import org.apache.wayang.core.api.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.fs.FileSystems;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;


public class ParquetFileSource extends UnarySource<DynamicMessage> {

    private final Logger logger = LogManager.getLogger(this.getClass());
    private final String inputUrl;
    private final Descriptor descriptor; // Protobuf Descriptor

    public ParquetFileSource(String inputUrl, Descriptor descriptor) {
        super(DataSetType.createDefault(DynamicMessage.class));
        this.inputUrl = inputUrl;
        this.descriptor = descriptor;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public ParquetFileSource(ParquetFileSource that) {
        super(that);
        this.inputUrl = that.getInputUrl();
        this.descriptor = that.getDescriptor();
    }

    public String getInputUrl() {
        return this.inputUrl;
    }

    public Descriptor getDescriptor() {
        return this.descriptor;
    }
    

    @Override
    public Optional<org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new ParquetFileSource.CardinalityEstimator());
    }

    /**
     * Custom {@link org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator} for Parquet files.
     */
    protected class CardinalityEstimator implements org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator {

        public final CardinalityEstimate FALLBACK_ESTIMATE = new CardinalityEstimate(1000L, 100000000L, 0.7);
        public static final double CORRECTNESS_PROBABILITY = 0.95d;
        public static final double EXPECTED_ESTIMATE_DEVIATION = 0.05;

        @Override
        public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
            Validate.isTrue(ParquetFileSource.this.getNumInputs() == inputEstimates.length);

            // See Job for StopWatch measurements
            final TimeMeasurement timeMeasurement = optimizationContext.getJob().getStopWatch().start(
                    "Optimization", "Cardinality&Load Estimation", "Push Estimation", "Estimate source cardinalities"
            );

            // Query the job cache first to see if there is already an estimate.
            String jobCacheKey = String.format("%s.estimate(%s)", this.getClass().getCanonicalName(), ParquetFileSource.this.inputUrl);
            CardinalityEstimate cardinalityEstimate = optimizationContext.queryJobCache(jobCacheKey, CardinalityEstimate.class);
            if (cardinalityEstimate != null) return cardinalityEstimate;

            // Otherwise calculate the cardinality.
            OptionalLong fileSize = FileSystems.getFileSize(ParquetFileSource.this.inputUrl);
            if (!fileSize.isPresent()) {
                ParquetFileSource.this.logger.warn("Could not determine size of {}... deliver fallback estimate.",
                        ParquetFileSource.this.inputUrl);
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;

            } else if (fileSize.getAsLong() == 0L) {
                timeMeasurement.stop();
                return new CardinalityEstimate(0L, 0L, 1d);
            }

            OptionalDouble rowsPerBlock = this.estimateRowsPerBlock();
            if (!rowsPerBlock.isPresent()) {
                ParquetFileSource.this.logger.warn("Could not determine average rows per block of {}... deliver fallback estimate.",
                        ParquetFileSource.this.inputUrl);
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;
            }

            // Extrapolate a cardinality estimate for the complete file.
            double numEstimatedRows = rowsPerBlock.getAsDouble() * fileSize.getAsLong();
            double expectedDeviation = numEstimatedRows * EXPECTED_ESTIMATE_DEVIATION;
            cardinalityEstimate = new CardinalityEstimate(
                    (long) (numEstimatedRows - expectedDeviation),
                    (long) (numEstimatedRows + expectedDeviation),
                    CORRECTNESS_PROBABILITY
            );

            // Cache the result, so that it will not be recalculated again.
            optimizationContext.putIntoJobCache(jobCacheKey, cardinalityEstimate);

            timeMeasurement.stop();
            return cardinalityEstimate;
        }

        /**
         * Estimate the number of rows per block in a Parquet file.
         *
         * @return the average number of rows per block if it could be determined
         */
        private OptionalDouble estimateRowsPerBlock() {
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            conf.set(ProtoReadSupport.PB_CLASS, ParquetFileSource.this.descriptor.getFullName());
            try (ParquetReader<DynamicMessage> reader = ProtoParquetReader.<DynamicMessage>builder(new Path(ParquetFileSource.this.inputUrl))
                    .withConf(conf)
                    .build()) {
                int rowsCount = 0;
                DynamicMessage record;
                while ((record = reader.read()) != null) {
                    rowsCount++;
                }

                if (rowsCount == 0) {
                    ParquetFileSource.this.logger.warn("Could not find any rows in {}.", ParquetFileSource.this.inputUrl);
                    return OptionalDouble.empty();
                }
                return OptionalDouble.of((double) rowsCount);
            } catch (IOException e) {
                ParquetFileSource.this.logger.error("Could not estimate rows per block of the input Parquet file.", e);
            }

            return OptionalDouble.empty();
        }
    }
}

