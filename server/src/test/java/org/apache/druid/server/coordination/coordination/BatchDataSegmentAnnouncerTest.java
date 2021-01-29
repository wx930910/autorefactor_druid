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

package org.apache.druid.server.coordination.coordination;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.druid.curator.PotentiallyGzippedCompressionProvider;
import org.apache.druid.curator.announcement.Announcer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordination.BatchDataSegmentAnnouncer;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.initialization.BatchDataSegmentAnnouncerConfig;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 *
 */
public class BatchDataSegmentAnnouncerTest {
	private static final String TEST_BASE_PATH = "/test";

	private static final int NUM_THREADS = 4;

	private TestingCluster testingCluster;
	private CuratorFramework cf;
	private ObjectMapper jsonMapper;
	private TestAnnouncer announcer;
	private BatchDataSegmentAnnouncer segmentAnnouncer;
	private Set<DataSegment> testSegments;

	private final AtomicInteger maxBytesPerNode = new AtomicInteger(512 * 1024);
	private Boolean skipDimensionsAndMetrics;
	private Boolean skipLoadSpec;

	private ExecutorService exec;

	@Before
	public void setUp() throws Exception {
		testingCluster = new TestingCluster(1);
		testingCluster.start();

		cf = CuratorFrameworkFactory.builder().connectString(testingCluster.getConnectString())
				.retryPolicy(new ExponentialBackoffRetry(1, 10))
				.compressionProvider(new PotentiallyGzippedCompressionProvider(false)).build();
		cf.start();
		cf.blockUntilConnected();
		cf.create().creatingParentsIfNeeded().forPath(TEST_BASE_PATH);

		jsonMapper = TestHelper.makeJsonMapper();

		announcer = new TestAnnouncer(cf, Execs.directExecutor());
		announcer.start();

		skipDimensionsAndMetrics = false;
		skipLoadSpec = false;
		segmentAnnouncer = new BatchDataSegmentAnnouncer(
				new DruidServerMetadata("id", "host", null, Long.MAX_VALUE, ServerType.HISTORICAL, "tier", 0),
				new BatchDataSegmentAnnouncerConfig() {
					@Override
					public int getSegmentsPerNode() {
						return 50;
					}

					@Override
					public long getMaxBytesPerNode() {
						return maxBytesPerNode.get();
					}

					@Override
					public boolean isSkipDimensionsAndMetrics() {
						return skipDimensionsAndMetrics;
					}

					@Override
					public boolean isSkipLoadSpec() {
						return skipLoadSpec;
					}
				}, new ZkPathsConfig() {
					@Override
					public String getBase() {
						return TEST_BASE_PATH;
					}
				}, announcer, jsonMapper);

		testSegments = new HashSet<>();
		for (int i = 0; i < 100; i++) {
			testSegments.add(makeSegment(i));
		}

		exec = Execs.multiThreaded(NUM_THREADS, "BatchDataSegmentAnnouncerTest-%d");
	}

	@After
	public void tearDown() throws Exception {
		announcer.stop();
		cf.close();
		testingCluster.stop();
		exec.shutdownNow();
	}

	@Test(timeout = 5000L)
	public void testAnnounceSegmentsWithSameSegmentConcurrently() throws ExecutionException, InterruptedException {
		final List<Future> futures = new ArrayList<>(NUM_THREADS);

		for (int i = 0; i < NUM_THREADS; i++) {
			futures.add(exec.submit(() -> {
				try {
					segmentAnnouncer.announceSegments(testSegments);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}));
		}

		for (Future future : futures) {
			future.get();
		}

		// Announcing 100 segments requires 2 nodes because of maxBytesPerNode
		// configuration.
		Assert.assertEquals(2, announcer.numPathAnnounced.size());
		for (ConcurrentHashMap<byte[], AtomicInteger> eachMap : announcer.numPathAnnounced.values()) {
			for (Entry<byte[], AtomicInteger> entry : eachMap.entrySet()) {
				Assert.assertEquals(1, entry.getValue().get());
			}
		}
	}

	private DataSegment makeSegment(int offset) {
		return DataSegment.builder().dataSource("foo")
				.interval(new Interval(DateTimes.of("2013-01-01").plusDays(offset),
						DateTimes.of("2013-01-02").plusDays(offset)))
				.version(DateTimes.nowUtc().toString()).dimensions(ImmutableList.of("dim1", "dim2"))
				.metrics(ImmutableList.of("met1", "met2")).loadSpec(ImmutableMap.of("type", "local")).size(0).build();
	}

	private static class TestAnnouncer extends Announcer {
		private final ConcurrentHashMap<String, ConcurrentHashMap<byte[], AtomicInteger>> numPathAnnounced = new ConcurrentHashMap<>();

		private TestAnnouncer(CuratorFramework curator, ExecutorService exec) {
			super(curator, exec);
		}

		@Override
		public void announce(String path, byte[] bytes, boolean removeParentIfCreated) {
			numPathAnnounced.computeIfAbsent(path, k -> new ConcurrentHashMap<>())
					.computeIfAbsent(bytes, k -> new AtomicInteger(0)).incrementAndGet();
			super.announce(path, bytes, removeParentIfCreated);
		}
	}
}
