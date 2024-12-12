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

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.LimitedInputStream;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.core.api.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.fs.FileSystems;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

public class ParquetFileSource extends UnarySource<GenericRecord> {

    private final Logger logger = LogManager.getLogger(this.getClass());
    private final String inputUrl;

    public ParquetFileSource(String inputUrl) {
        super(DataSetType.createDefault(GenericRecord.class));
        this.inputUrl = inputUrl;
    }

    public ParquetFileSource(ParquetFileSource that) {
        super(that);
        this.inputUrl = that.getInputUrl();
    }

    public String getInputUrl() {
        return this.inputUrl;
    }

    @Override
    public Optional<org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator> createCardinalityEstimator(
        final int outputIndex,
        final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new ParquetFileSource.CardinalityEstimator());
    }

    /**
     * Custom {@link org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator} for {@link FlatMapOperator}s.
     */
    protected class CardinalityEstimator implements org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator {

        public final CardinalityEstimate FALLBACK_ESTIMATE = new CardinalityEstimate(1000L, 100000000L, 0.7);

        public static final double CORRECTNESS_PROBABILITY = 0.95d;

        /**
         * We expect selectivities to be correct within a factor of {@value #EXPECTED_ESTIMATE_DEVIATION}.
         */
        public static final double EXPECTED_ESTIMATE_DEVIATION = 0.05;

        @Override
        public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
            Validate.isTrue(ParquetFileSource.this.getNumInputs() == inputEstimates.length);

            // see Job for StopWatch measurements
            final TimeMeasurement timeMeasurement = optimizationContext.getJob().getStopWatch().start(
                    "Optimization", "Cardinality&Load Estimation", "Push Estimation", "Estimate source cardinalities"
            );

            // Query the job cache first to see if there is already an estimate.
            String jobCacheKey = String.format("%s.estimate(%s)", this.getClass().getCanonicalName(), ParquetFileSource.this.inputUrl);
            CardinalityEstimate cardinalityEstimate = optimizationContext.queryJobCache(jobCacheKey, CardinalityEstimate.class);
            if (cardinalityEstimate != null) return  cardinalityEstimate;

            // Otherwise calculate the cardinality.
            // First, inspect the size of the file and its line sizes.
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

            OptionalDouble bytesPerLine = this.estimateBytesPerLine();
            if (!bytesPerLine.isPresent()) {
                ParquetFileSource.this.logger.warn("Could not determine average line size of {}... deliver fallback estimate.",
                        ParquetFileSource.this.inputUrl);
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;
            }

            // Extrapolate a cardinality estimate for the complete file.
            double numEstimatedLines = fileSize.getAsLong() / bytesPerLine.getAsDouble();
            double expectedDeviation = numEstimatedLines * EXPECTED_ESTIMATE_DEVIATION;
            cardinalityEstimate = new CardinalityEstimate(
                    (long) (numEstimatedLines - expectedDeviation),
                    (long) (numEstimatedLines + expectedDeviation),
                    CORRECTNESS_PROBABILITY
            );

            // Cache the result, so that it will not be recalculated again.
            optimizationContext.putIntoJobCache(jobCacheKey, cardinalityEstimate);

            timeMeasurement.stop();
            return cardinalityEstimate;
        }

        /**
         * Estimate the number of bytes that are in each line of a given file.
         *
         * @return the average number of bytes per line if it could be determined
         */
        private OptionalDouble estimateBytesPerLine() {
            return OptionalDouble.of(10);
        }
    }
}