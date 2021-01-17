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

package org.apache.druid.query.groupby.epinephelinae;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.epinephelinae.Grouper.BufferComparator;
import org.apache.druid.query.groupby.epinephelinae.Grouper.Entry;
import org.apache.druid.query.groupby.epinephelinae.Grouper.KeySerde;
import org.apache.druid.query.groupby.epinephelinae.Grouper.KeySerdeFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.MoreExecutors;

@RunWith(Parameterized.class)
public class ConcurrentGrouperTest {
	static public ColumnSelectorFactory mockColumnSelectorFactory1() {
		ColumnSelectorFactory mockInstance = Mockito.spy(ColumnSelectorFactory.class);
		try {
			Mockito.doAnswer((stubInvo) -> {
				return null;
			}).when(mockInstance).makeDimensionSelector(Mockito.any(DimensionSpec.class));
			Mockito.doAnswer((stubInvo) -> {
				return null;
			}).when(mockInstance).getColumnCapabilities(Mockito.any(String.class));
			Mockito.doAnswer((stubInvo) -> {
				return null;
			}).when(mockInstance).makeColumnValueSelector(Mockito.any(String.class));
		} catch (Exception exception) {
		}
		return mockInstance;
	}

	static public KeySerdeFactory<Long> mockKeySerdeFactory1() {
		KeySerdeFactory<Long> mockInstance = Mockito.spy(KeySerdeFactory.class);
		try {
			Mockito.doAnswer((stubInvo) -> {
				return (long) 0;
			}).when(mockInstance).getMaxDictionarySize();
			Mockito.doAnswer((stubInvo) -> {
				return new KeySerde<Long>() {
					final ByteBuffer buffer = ByteBuffer.allocate(8);

					@Override
					public int keySize() {
						return 8;
					}

					@Override
					public Class<Long> keyClazz() {
						return Long.class;
					}

					@Override
					public List<String> getDictionary() {
						return ImmutableList.of();
					}

					@Override
					public ByteBuffer toByteBuffer(Long key) {
						buffer.rewind();
						buffer.putLong(key);
						buffer.position(0);
						return buffer;
					}

					@Override
					public Long fromByteBuffer(ByteBuffer buffer, int position) {
						return buffer.getLong(position);
					}

					@Override
					public BufferComparator bufferComparator() {
						return new BufferComparator() {
							@Override
							public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition,
									int rhsPosition) {
								return Longs.compare(lhsBuffer.getLong(lhsPosition), rhsBuffer.getLong(rhsPosition));
							}
						};
					}

					@Override
					public BufferComparator bufferComparatorWithAggregators(AggregatorFactory[] aggregatorFactories,
							int[] aggregatorOffsets) {
						return null;
					}

					@Override
					public void reset() {
					}
				};
			}).when(mockInstance).factorize();
			Mockito.doAnswer((stubInvo) -> {
				return mockInstance.factorize();
			}).when(mockInstance).factorizeWithDictionary(Mockito.any(List.class));
			Mockito.doAnswer((stubInvo) -> {
				return new Comparator<Grouper.Entry<Long>>() {
					@Override
					public int compare(Grouper.Entry<Long> o1, Grouper.Entry<Long> o2) {
						return o1.getKey().compareTo(o2.getKey());
					}
				};
			}).when(mockInstance).objectComparator(Mockito.anyBoolean());
		} catch (Exception exception) {
		}
		return mockInstance;
	}

	static public ReferenceCountingResourceHolder<ByteBuffer> mockReferenceCountingResourceHolder1(int bufferSize) {
		ReferenceCountingResourceHolder<ByteBuffer> mockInstance = Mockito
				.spy(new ReferenceCountingResourceHolder(ByteBuffer.allocate(bufferSize), () -> {
				}));
		try {
			Mockito.doAnswer((stubInvo) -> {
				return stubInvo.callRealMethod();
			}).when(mockInstance).get();
		} catch (Exception exception) {
		}
		return mockInstance;
	}

	@Before()
	public void initiateMockedFields() {
		try {
			Mockito.doAnswer((stubInvo) -> {
				return stubInvo.callRealMethod();
			}).when(TEST_RESOURCE_HOLDER).get();
		} catch (Exception exception) {
		}
	}

	private static final ExecutorService SERVICE = Executors.newFixedThreadPool(8);
	private static final ReferenceCountingResourceHolder<ByteBuffer> TEST_RESOURCE_HOLDER = Mockito
			.spy(new ReferenceCountingResourceHolder(ByteBuffer.allocate(256), () -> {
			}));
	private static final KeySerdeFactory<Long> KEY_SERDE_FACTORY = ConcurrentGrouperTest.mockKeySerdeFactory1();
	private static final ColumnSelectorFactory NULL_FACTORY = ConcurrentGrouperTest.mockColumnSelectorFactory1();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Supplier<ByteBuffer> bufferSupplier;

	@Parameters(name = "bufferSize={0}")
	public static Collection<Object[]> constructorFeeder() {
		return ImmutableList.of(new Object[] { 1024 * 32 }, new Object[] { 1024 * 1024 });
	}

	@AfterClass
	public static void teardown() {
		SERVICE.shutdown();
	}

	public ConcurrentGrouperTest(int bufferSize) {
		bufferSupplier = new Supplier<ByteBuffer>() {
			private final AtomicBoolean called = new AtomicBoolean(false);
			private ByteBuffer buffer;

			@Override
			public ByteBuffer get() {
				if (called.compareAndSet(false, true)) {
					buffer = ByteBuffer.allocate(bufferSize);
				}

				return buffer;
			}
		};
	}

	@Test()
	public void testAggregate() throws InterruptedException, ExecutionException, IOException {
		final ConcurrentGrouper<Long> grouper = new ConcurrentGrouper<>(bufferSupplier, TEST_RESOURCE_HOLDER,
				KEY_SERDE_FACTORY, KEY_SERDE_FACTORY, NULL_FACTORY,
				new AggregatorFactory[] { new CountAggregatorFactory("cnt") }, 1024, 0.7f, 1,
				new LimitedTemporaryStorage(temporaryFolder.newFolder(), 1024 * 1024), new DefaultObjectMapper(), 8,
				null, false, MoreExecutors.listeningDecorator(SERVICE), 0, false, 0, 4, 8);
		grouper.init();

		final int numRows = 1000;

		Future<?>[] futures = new Future[8];

		for (int i = 0; i < 8; i++) {
			futures[i] = SERVICE.submit(new Runnable() {
				@Override
				public void run() {
					for (long i = 0; i < numRows; i++) {
						grouper.aggregate(i);
					}
				}
			});
		}

		for (Future eachFuture : futures) {
			eachFuture.get();
		}

		final CloseableIterator<Entry<Long>> iterator = grouper.iterator(true);
		final List<Entry<Long>> actual = Lists.newArrayList(iterator);
		iterator.close();

		Mockito.verify(TEST_RESOURCE_HOLDER, Mockito.atLeastOnce()).get();

		final List<Entry<Long>> expected = new ArrayList<>();
		for (long i = 0; i < numRows; i++) {
			expected.add(new Entry<>(i, new Object[] { 8L }));
		}

		Assert.assertEquals(expected, actual);

		grouper.close();
	}
}
