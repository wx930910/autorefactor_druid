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

package org.apache.druid.segment.serde;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.mockito.Mockito;

import com.google.common.hash.HashFunction;

import it.unimi.dsi.fastutil.bytes.ByteArrays;

public class HyperUniquesSerdeForTest {
	static public ComplexMetricSerde mockComplexMetricSerde1(HashFunction hashFn) {
		Comparator<HyperLogLogCollector> mockFieldVariableComparator = Comparator
				.nullsFirst(Comparator.comparing(HyperLogLogCollector::toByteBuffer));
		HashFunction[] mockFieldVariableHashFn = new HashFunction[1];
		ComplexMetricSerde mockInstance = Mockito.spy(ComplexMetricSerde.class);
		mockFieldVariableHashFn[0] = hashFn;
		try {
			Mockito.doAnswer((stubInvo) -> {
				return "hyperUnique";
			}).when(mockInstance).getTypeName();
			Mockito.doAnswer((stubInvo) -> {
				return new ComplexMetricExtractor() {
					@Override
					public Class<HyperLogLogCollector> extractedClass() {
						return HyperLogLogCollector.class;
					}

					@Override
					public HyperLogLogCollector extractValue(InputRow inputRow, String metricName) {
						Object rawValue = inputRow.getRaw(metricName);
						if (rawValue instanceof HyperLogLogCollector) {
							return (HyperLogLogCollector) rawValue;
						} else {
							HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
							List<String> dimValues = inputRow.getDimension(metricName);
							if (dimValues == null) {
								return collector;
							}
							for (String dimensionValue : dimValues) {
								collector.add(mockFieldVariableHashFn[0].hashBytes(StringUtils.toUtf8(dimensionValue))
										.asBytes());
							}
							return collector;
						}
					}
				};
			}).when(mockInstance).getExtractor();
			Mockito.doAnswer((stubInvo) -> {
				SegmentWriteOutMedium segmentWriteOutMedium = stubInvo.getArgument(0);
				String metric = stubInvo.getArgument(1);
				return LargeColumnSupportedComplexColumnSerializer.createWithColumnSize(segmentWriteOutMedium, metric,
						mockInstance.getObjectStrategy(), Integer.MAX_VALUE);
			}).when(mockInstance).getSerializer(Mockito.any(SegmentWriteOutMedium.class), Mockito.any(String.class));
			Mockito.doAnswer((stubInvo) -> {
				ByteBuffer byteBuffer = stubInvo.getArgument(0);
				ColumnBuilder columnBuilder = stubInvo.getArgument(1);
				final GenericIndexed column;
				if (columnBuilder.getFileMapper() == null) {
					column = GenericIndexed.read(byteBuffer, mockInstance.getObjectStrategy());
				} else {
					column = GenericIndexed.read(byteBuffer, mockInstance.getObjectStrategy(),
							columnBuilder.getFileMapper());
				}
				columnBuilder
						.setComplexColumnSupplier(new ComplexColumnPartSupplier(mockInstance.getTypeName(), column));
				return null;
			}).when(mockInstance).deserializeColumn(Mockito.any(ByteBuffer.class), Mockito.any(ColumnBuilder.class));
			Mockito.doAnswer((stubInvo) -> {
				return new ObjectStrategy<HyperLogLogCollector>() {
					@Override
					public Class<? extends HyperLogLogCollector> getClazz() {
						return HyperLogLogCollector.class;
					}

					@Override
					public HyperLogLogCollector fromByteBuffer(ByteBuffer buffer, int numBytes) {
						final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
						readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
						return HyperLogLogCollector.makeCollector(readOnlyBuffer);
					}

					@Override
					public byte[] toBytes(HyperLogLogCollector collector) {
						if (collector == null) {
							return ByteArrays.EMPTY_ARRAY;
						}
						ByteBuffer val = collector.toByteBuffer();
						byte[] retVal = new byte[val.remaining()];
						val.asReadOnlyBuffer().get(retVal);
						return retVal;
					}

					@Override
					public int compare(HyperLogLogCollector o1, HyperLogLogCollector o2) {
						return mockFieldVariableComparator.compare(o1, o2);
					}
				};
			}).when(mockInstance).getObjectStrategy();
		} catch (Exception exception) {
		}
		return mockInstance;
	}
}
