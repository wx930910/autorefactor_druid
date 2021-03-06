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

package org.apache.druid.query.groupby.having;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.groupby.ResultRow;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class HavingSpecTest {
	static public HavingSpec mockHavingSpec1(AtomicInteger counter, boolean value) {
		boolean[] mockFieldVariableValue = new boolean[1];
		AtomicInteger[] mockFieldVariableCounter = new AtomicInteger[1];
		HavingSpec mockInstance = Mockito.spy(HavingSpec.class);
		mockFieldVariableCounter[0] = counter;
		mockFieldVariableValue[0] = value;
		try {
			Mockito.doAnswer((stubInvo) -> {
				mockFieldVariableCounter[0].incrementAndGet();
				return mockFieldVariableValue[0];
			}).when(mockInstance).eval(Mockito.any(ResultRow.class));
			Mockito.doAnswer((stubInvo) -> {
				return new CacheKeyBuilder(HavingSpecUtil.CACHE_TYPE_ID_COUNTING)
						.appendByte((byte) (mockFieldVariableValue[0] ? 1 : 0))
						.appendByteArray(StringUtils.toUtf8(String.valueOf(mockFieldVariableCounter[0]))).build();
			}).when(mockInstance).getCacheKey();
		} catch (Exception exception) {
		}
		return mockInstance;
	}

	private static final ResultRow ROW = ResultRow.of(10f);

	@Test
	public void testHavingClauseSerde() {
		List<HavingSpec> havings = Arrays.asList(new GreaterThanHavingSpec("agg", 1.3),
				new OrHavingSpec(Arrays.asList(new LessThanHavingSpec("lessAgg", 1L),
						new NotHavingSpec(new EqualToHavingSpec("equalAgg", 2.0)))));

		HavingSpec andHavingSpec = new AndHavingSpec(havings);

		Map<String, Object> notMap = ImmutableMap.of("type", "not", "havingSpec",
				ImmutableMap.of("type", "equalTo", "aggregation", "equalAgg", "value", 2.0));

		Map<String, Object> lessMap = ImmutableMap.of("type", "lessThan", "aggregation", "lessAgg", "value", 1);

		Map<String, Object> greaterMap = ImmutableMap.of("type", "greaterThan", "aggregation", "agg", "value", 1.3);

		Map<String, Object> orMap = ImmutableMap.of("type", "or", "havingSpecs", ImmutableList.of(lessMap, notMap));

		Map<String, Object> payloadMap = ImmutableMap.of("type", "and", "havingSpecs",
				ImmutableList.of(greaterMap, orMap));

		ObjectMapper mapper = new DefaultObjectMapper();
		Assert.assertEquals(andHavingSpec, mapper.convertValue(payloadMap, AndHavingSpec.class));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testTypeTypo() {
		Map<String, Object> greaterMap = ImmutableMap.of("type", "nonExistingType", "aggregation", "agg", "value", 1.3);
		ObjectMapper mapper = new DefaultObjectMapper();
		// noinspection unused
		HavingSpec spec = mapper.convertValue(greaterMap, HavingSpec.class);
	}

	@Test
	public void testGreaterThanHavingSpec() {
		GreaterThanHavingSpec spec = new GreaterThanHavingSpec("metric", Long.MAX_VALUE - 10);
		Assert.assertFalse(spec.eval(getTestRow(Long.MAX_VALUE - 10)));
		Assert.assertFalse(spec.eval(getTestRow(Long.MAX_VALUE - 15)));
		Assert.assertTrue(spec.eval(getTestRow(Long.MAX_VALUE - 5)));
		Assert.assertTrue(spec.eval(getTestRow(String.valueOf(Long.MAX_VALUE - 5))));
		Assert.assertFalse(spec.eval(getTestRow(100.05f)));

		spec = new GreaterThanHavingSpec("metric", 100.56f);
		Assert.assertFalse(spec.eval(getTestRow(100.56f)));
		Assert.assertFalse(spec.eval(getTestRow(90.53f)));
		Assert.assertFalse(spec.eval(getTestRow("90.53f")));
		Assert.assertTrue(spec.eval(getTestRow(101.34f)));
		Assert.assertTrue(spec.eval(getTestRow(Long.MAX_VALUE)));
	}

	@Test
	public void testLessThanHavingSpec() {
		LessThanHavingSpec spec = new LessThanHavingSpec("metric", Long.MAX_VALUE - 10);
		Assert.assertFalse(spec.eval(getTestRow(Long.MAX_VALUE - 10)));
		Assert.assertTrue(spec.eval(getTestRow(Long.MAX_VALUE - 15)));
		Assert.assertTrue(spec.eval(getTestRow(String.valueOf(Long.MAX_VALUE - 15))));
		Assert.assertFalse(spec.eval(getTestRow(Long.MAX_VALUE - 5)));
		Assert.assertTrue(spec.eval(getTestRow(100.05f)));

		spec = new LessThanHavingSpec("metric", 100.56f);
		Assert.assertFalse(spec.eval(getTestRow(100.56f)));
		Assert.assertTrue(spec.eval(getTestRow(90.53f)));
		Assert.assertFalse(spec.eval(getTestRow(101.34f)));
		Assert.assertFalse(spec.eval(getTestRow("101.34f")));
		Assert.assertFalse(spec.eval(getTestRow(Long.MAX_VALUE)));
	}

	private ResultRow getTestRow(Object metricValue) {
		return ResultRow.of(metricValue);
	}

	@Test
	public void testEqualHavingSpec() {
		EqualToHavingSpec spec = new EqualToHavingSpec("metric", Long.MAX_VALUE - 10);
		Assert.assertTrue(spec.eval(getTestRow(Long.MAX_VALUE - 10)));
		Assert.assertFalse(spec.eval(getTestRow(Long.MAX_VALUE - 5)));
		Assert.assertFalse(spec.eval(getTestRow(100.05f)));

		spec = new EqualToHavingSpec("metric", 100.56f);
		Assert.assertFalse(spec.eval(getTestRow(100L)));
		Assert.assertFalse(spec.eval(getTestRow(100.0)));
		Assert.assertFalse(spec.eval(getTestRow(100d)));
		Assert.assertFalse(spec.eval(getTestRow(100.56d))); // False since 100.56d != (double) 100.56f
		Assert.assertFalse(spec.eval(getTestRow(90.53d)));
		Assert.assertTrue(spec.eval(getTestRow(100.56f)));
		Assert.assertFalse(spec.eval(getTestRow(90.53f)));
		Assert.assertFalse(spec.eval(getTestRow(Long.MAX_VALUE)));

		spec = new EqualToHavingSpec("metric", 100.56d);
		Assert.assertFalse(spec.eval(getTestRow(100L)));
		Assert.assertFalse(spec.eval(getTestRow(100.0)));
		Assert.assertFalse(spec.eval(getTestRow(100d)));
		Assert.assertTrue(spec.eval(getTestRow(100.56d)));
		Assert.assertFalse(spec.eval(getTestRow(90.53d)));
		Assert.assertFalse(spec.eval(getTestRow(100.56f))); // False since 100.56d != (double) 100.56f
		Assert.assertFalse(spec.eval(getTestRow(90.53f)));
		Assert.assertFalse(spec.eval(getTestRow(Long.MAX_VALUE)));

		spec = new EqualToHavingSpec("metric", 100.0f);
		Assert.assertTrue(spec.eval(getTestRow(100L)));
		Assert.assertTrue(spec.eval(getTestRow(100.0)));
		Assert.assertTrue(spec.eval(getTestRow(100d)));
		Assert.assertFalse(spec.eval(getTestRow(100.56d)));
		Assert.assertFalse(spec.eval(getTestRow(90.53d)));
		Assert.assertFalse(spec.eval(getTestRow(100.56f)));
		Assert.assertFalse(spec.eval(getTestRow(90.53f)));
		Assert.assertFalse(spec.eval(getTestRow(Long.MAX_VALUE)));

		spec = new EqualToHavingSpec("metric", 100.0d);
		Assert.assertTrue(spec.eval(getTestRow(100L)));
		Assert.assertTrue(spec.eval(getTestRow(100.0)));
		Assert.assertTrue(spec.eval(getTestRow(100d)));
		Assert.assertFalse(spec.eval(getTestRow(100.56d)));
		Assert.assertFalse(spec.eval(getTestRow(90.53d)));
		Assert.assertFalse(spec.eval(getTestRow(100.56f)));
		Assert.assertFalse(spec.eval(getTestRow(90.53f)));
		Assert.assertFalse(spec.eval(getTestRow(Long.MAX_VALUE)));

		spec = new EqualToHavingSpec("metric", 100);
		Assert.assertTrue(spec.eval(getTestRow(100L)));
		Assert.assertTrue(spec.eval(getTestRow(100.0)));
		Assert.assertTrue(spec.eval(getTestRow(100d)));
		Assert.assertFalse(spec.eval(getTestRow(100.56d)));
		Assert.assertFalse(spec.eval(getTestRow(90.53d)));
		Assert.assertFalse(spec.eval(getTestRow(100.56f)));
		Assert.assertFalse(spec.eval(getTestRow(90.53f)));
		Assert.assertFalse(spec.eval(getTestRow(Long.MAX_VALUE)));

		spec = new EqualToHavingSpec("metric", 100L);
		Assert.assertTrue(spec.eval(getTestRow(100L)));
		Assert.assertTrue(spec.eval(getTestRow(100.0)));
		Assert.assertTrue(spec.eval(getTestRow(100d)));
		Assert.assertFalse(spec.eval(getTestRow(100.56d)));
		Assert.assertFalse(spec.eval(getTestRow(90.53d)));
		Assert.assertFalse(spec.eval(getTestRow(100.56f)));
		Assert.assertFalse(spec.eval(getTestRow(90.53f)));
		Assert.assertFalse(spec.eval(getTestRow(Long.MAX_VALUE)));
	}

	@Test
	public void testAndHavingSpecShouldSupportShortcutEvaluation() {
		AtomicInteger counter = new AtomicInteger(0);
		AndHavingSpec spec = new AndHavingSpec(ImmutableList.of(HavingSpecTest.mockHavingSpec1(counter, true),
				HavingSpecTest.mockHavingSpec1(counter, false), HavingSpecTest.mockHavingSpec1(counter, true),
				HavingSpecTest.mockHavingSpec1(counter, false)));

		spec.eval(ROW);

		Assert.assertEquals(2, counter.get());
	}

	@Test
	public void testAndHavingSpec() {
		AtomicInteger counter = new AtomicInteger(0);
		AndHavingSpec spec = new AndHavingSpec(ImmutableList.of(HavingSpecTest.mockHavingSpec1(counter, true),
				HavingSpecTest.mockHavingSpec1(counter, true), HavingSpecTest.mockHavingSpec1(counter, true),
				HavingSpecTest.mockHavingSpec1(counter, true)));

		spec.eval(ROW);

		Assert.assertEquals(4, counter.get());

		counter.set(0);
		spec = new AndHavingSpec(ImmutableList.of(HavingSpecTest.mockHavingSpec1(counter, false),
				HavingSpecTest.mockHavingSpec1(counter, true), HavingSpecTest.mockHavingSpec1(counter, true),
				HavingSpecTest.mockHavingSpec1(counter, true)));

		spec.eval(ROW);

		Assert.assertEquals(1, counter.get());
	}

	@Test
	public void testOrHavingSpecSupportsShortcutEvaluation() {
		AtomicInteger counter = new AtomicInteger(0);
		OrHavingSpec spec = new OrHavingSpec(ImmutableList.of(HavingSpecTest.mockHavingSpec1(counter, true),
				HavingSpecTest.mockHavingSpec1(counter, true), HavingSpecTest.mockHavingSpec1(counter, true),
				HavingSpecTest.mockHavingSpec1(counter, false)));

		spec.eval(ROW);

		Assert.assertEquals(1, counter.get());
	}

	@Test
	public void testOrHavingSpec() {
		AtomicInteger counter = new AtomicInteger(0);
		OrHavingSpec spec = new OrHavingSpec(ImmutableList.of(HavingSpecTest.mockHavingSpec1(counter, false),
				HavingSpecTest.mockHavingSpec1(counter, false), HavingSpecTest.mockHavingSpec1(counter, false),
				HavingSpecTest.mockHavingSpec1(counter, false)));

		spec.eval(ROW);

		Assert.assertEquals(4, counter.get());

		counter.set(0);
		spec = new OrHavingSpec(ImmutableList.of(HavingSpecTest.mockHavingSpec1(counter, false),
				HavingSpecTest.mockHavingSpec1(counter, false), HavingSpecTest.mockHavingSpec1(counter, false),
				HavingSpecTest.mockHavingSpec1(counter, true)));

		spec.eval(ROW);

		Assert.assertEquals(4, counter.get());
	}

	@Test
	public void testNotHavingSepc() {
		NotHavingSpec spec = new NotHavingSpec(new NeverHavingSpec());
		Assert.assertTrue(spec.eval(ROW));

		spec = new NotHavingSpec(new AlwaysHavingSpec());
		Assert.assertFalse(spec.eval(ROW));
	}
}
