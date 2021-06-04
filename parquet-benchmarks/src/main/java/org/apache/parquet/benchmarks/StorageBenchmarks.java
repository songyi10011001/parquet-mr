/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.benchmarks;

import static org.apache.parquet.benchmarks.BenchmarkConstants.BLOCK_SIZE_DEFAULT;
import static org.apache.parquet.benchmarks.BenchmarkConstants.FIXED_LEN_BYTEARRAY_SIZE;
import static org.apache.parquet.benchmarks.BenchmarkConstants.ONE_MILLION;
import static org.apache.parquet.benchmarks.BenchmarkConstants.PAGE_SIZE_DEFAULT;
import static org.apache.parquet.benchmarks.BenchmarkFiles.TARGET_DIR;
import static org.apache.parquet.benchmarks.BenchmarkFiles.configuration;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M_SNAPPY;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static org.openjdk.jmh.annotations.Scope.Thread;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Thread)
public class StorageBenchmarks {
    private DataGenerator dataGenerator = new DataGenerator();

    private static final Path file_1M_SNAPPY_ENCRYPT = new Path(TARGET_DIR + "/PARQUET-1M-SNAPPY-ENCRYPTED");

    @Setup(Level.Iteration)
    public void setup() {
	// clean existing test data at the beginning of each iteration
	dataGenerator.cleanup();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void write1MRowsDefaultBlockAndPageSizeSNAPPY() throws IOException {
	dataGenerator.generateData(file_1M_SNAPPY, configuration, PARQUET_2_0, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT,
		FIXED_LEN_BYTEARRAY_SIZE, SNAPPY, ONE_MILLION);

	dataGenerator.generateData(file_1M_SNAPPY_ENCRYPT, configuration, PARQUET_2_0, BLOCK_SIZE_DEFAULT,
		PAGE_SIZE_DEFAULT, FIXED_LEN_BYTEARRAY_SIZE, SNAPPY, ONE_MILLION, true);
    }
}
