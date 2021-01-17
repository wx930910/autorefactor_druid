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

package org.apache.druid.client;

import java.util.Map;

import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Druids;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.Result;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CacheUtilTest {
	static public CacheStrategy<Result<TimeseriesResultValue>, Object, Query<Result<TimeseriesResultValue>>> mockCacheStrategy1(
			boolean cacheableOnBrokers, boolean cacheableOnDataServers) {
		boolean[] mockFieldVariableCacheableOnDataServers = new boolean[1];
		boolean[] mockFieldVariableCacheableOnBrokers = new boolean[1];
		CacheStrategy<Result<TimeseriesResultValue>, Object, Query<Result<TimeseriesResultValue>>> mockInstance = Mockito
				.spy(CacheStrategy.class);
		mockFieldVariableCacheableOnBrokers[0] = cacheableOnBrokers;
		mockFieldVariableCacheableOnDataServers[0] = cacheableOnDataServers;
		try {
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).computeCacheKey(Mockito.any(Query.class));
			Mockito.doAnswer((stubInvo) -> {
				boolean willMergeRunners = stubInvo.getArgument(1);
				return willMergeRunners ? mockFieldVariableCacheableOnDataServers[0]
						: mockFieldVariableCacheableOnBrokers[0];
			}).when(mockInstance).isCacheable(Mockito.any(Query.class), Mockito.anyBoolean());
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).getCacheObjectClazz();
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).computeResultLevelCacheKey(Mockito.any(Query.class));
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).prepareForCache(Mockito.anyBoolean());
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).pullFromCache(Mockito.anyBoolean());
		} catch (Exception exception) {
		}
		return mockInstance;
	}

	private final TimeseriesQuery timeseriesQuery = Druids.newTimeseriesQueryBuilder().dataSource("foo")
			.intervals("2000/3000").granularity(Granularities.ALL).build();

	@Test
	public void test_isQueryCacheable_cacheableOnBroker() {
		Assert.assertTrue(CacheUtil.isQueryCacheable(timeseriesQuery, CacheUtilTest.mockCacheStrategy1(true, true),
				makeCacheConfig(ImmutableMap.of()), CacheUtil.ServerType.BROKER));
	}

	@Test
	public void test_isQueryCacheable_cacheableOnDataServer() {
		Assert.assertTrue(CacheUtil.isQueryCacheable(timeseriesQuery, CacheUtilTest.mockCacheStrategy1(true, true),
				makeCacheConfig(ImmutableMap.of()), CacheUtil.ServerType.DATA));
	}

	@Test
	public void test_isQueryCacheable_unCacheableOnBroker() {
		Assert.assertFalse(CacheUtil.isQueryCacheable(timeseriesQuery, CacheUtilTest.mockCacheStrategy1(false, true),
				makeCacheConfig(ImmutableMap.of()), CacheUtil.ServerType.BROKER));
	}

	@Test
	public void test_isQueryCacheable_unCacheableOnDataServer() {
		Assert.assertFalse(CacheUtil.isQueryCacheable(timeseriesQuery, CacheUtilTest.mockCacheStrategy1(true, false),
				makeCacheConfig(ImmutableMap.of()), CacheUtil.ServerType.DATA));
	}

	@Test
	public void test_isQueryCacheable_unCacheableType() {
		Assert.assertFalse(CacheUtil.isQueryCacheable(timeseriesQuery, CacheUtilTest.mockCacheStrategy1(true, false),
				makeCacheConfig(ImmutableMap.of("unCacheable", ImmutableList.of("timeseries"))),
				CacheUtil.ServerType.BROKER));
	}

	@Test
	public void test_isQueryCacheable_unCacheableDataSource() {
		Assert.assertFalse(CacheUtil.isQueryCacheable(timeseriesQuery.withDataSource(new LookupDataSource("lookyloo")),
				CacheUtilTest.mockCacheStrategy1(true, true), makeCacheConfig(ImmutableMap.of()),
				CacheUtil.ServerType.BROKER));
	}

	@Test
	public void test_isQueryCacheable_nullCacheStrategy() {
		Assert.assertFalse(CacheUtil.isQueryCacheable(timeseriesQuery, null, makeCacheConfig(ImmutableMap.of()),
				CacheUtil.ServerType.BROKER));
	}

	private static CacheConfig makeCacheConfig(final Map<String, Object> properties) {
		return TestHelper.makeJsonMapper().convertValue(properties, CacheConfig.class);
	}
}
