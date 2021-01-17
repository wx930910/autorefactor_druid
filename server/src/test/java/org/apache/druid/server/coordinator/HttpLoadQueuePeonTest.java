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

package org.apache.druid.server.coordinator;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.server.ServerTestHelper;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentLoadDropHandler;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.Duration;
import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 */
public class HttpLoadQueuePeonTest {
	public DruidCoordinatorConfig mockDruidCoordinatorConfig1(Duration coordinatorStartDelay,
			Duration coordinatorPeriod, Duration coordinatorIndexingPeriod, Duration loadTimeoutDelay,
			Duration coordinatorKillPeriod, Duration coordinatorKillDurationToRetain, int coordinatorKillMaxSegments,
			Duration getLoadQueuePeonRepeatDelay) {
		Duration[] mockFieldVariableLoadTimeoutDelay = new Duration[1];
		int[] mockFieldVariableCoordinatorKillMaxSegments = new int[1];
		Duration[] mockFieldVariableGetLoadQueuePeonRepeatDelay = new Duration[1];
		Duration[] mockFieldVariableCoordinatorKillDurationToRetain = new Duration[1];
		Duration[] mockFieldVariableCoordinatorStartDelay = new Duration[1];
		Duration[] mockFieldVariableCoordinatorIndexingPeriod = new Duration[1];
		Duration[] mockFieldVariableCoordinatorPeriod = new Duration[1];
		Duration[] mockFieldVariableCoordinatorKillPeriod = new Duration[1];
		DruidCoordinatorConfig mockInstance = Mockito.spy(DruidCoordinatorConfig.class);
		mockFieldVariableCoordinatorStartDelay[0] = coordinatorStartDelay;
		mockFieldVariableCoordinatorPeriod[0] = coordinatorPeriod;
		mockFieldVariableCoordinatorIndexingPeriod[0] = coordinatorIndexingPeriod;
		mockFieldVariableLoadTimeoutDelay[0] = loadTimeoutDelay;
		mockFieldVariableCoordinatorKillPeriod[0] = coordinatorKillPeriod;
		mockFieldVariableCoordinatorKillDurationToRetain[0] = coordinatorKillDurationToRetain;
		mockFieldVariableCoordinatorKillMaxSegments[0] = coordinatorKillMaxSegments;
		mockFieldVariableGetLoadQueuePeonRepeatDelay[0] = getLoadQueuePeonRepeatDelay;
		try {
			Mockito.doAnswer((stubInvo) -> {
				return mockFieldVariableCoordinatorStartDelay[0];
			}).when(mockInstance).getCoordinatorStartDelay();
			Mockito.doAnswer((stubInvo) -> {
				return mockFieldVariableCoordinatorKillDurationToRetain[0];
			}).when(mockInstance).getCoordinatorKillDurationToRetain();
			Mockito.doAnswer((stubInvo) -> {
				return mockFieldVariableGetLoadQueuePeonRepeatDelay[0];
			}).when(mockInstance).getLoadQueuePeonRepeatDelay();
			Mockito.doAnswer((stubInvo) -> {
				return mockFieldVariableCoordinatorKillPeriod[0];
			}).when(mockInstance).getCoordinatorKillPeriod();
			Mockito.doAnswer((stubInvo) -> {
				return 2;
			}).when(mockInstance).getHttpLoadQueuePeonBatchSize();
			Mockito.doAnswer((stubInvo) -> {
				return mockFieldVariableCoordinatorPeriod[0];
			}).when(mockInstance).getCoordinatorPeriod();
			Mockito.doAnswer((stubInvo) -> {
				return mockFieldVariableCoordinatorIndexingPeriod[0];
			}).when(mockInstance).getCoordinatorIndexingPeriod();
			Mockito.doAnswer((stubInvo) -> {
				return mockFieldVariableCoordinatorKillMaxSegments[0];
			}).when(mockInstance).getCoordinatorKillMaxSegments();
			Mockito.doAnswer((stubInvo) -> {
				return mockFieldVariableLoadTimeoutDelay[0] == null ? stubInvo.callRealMethod()
						: mockFieldVariableLoadTimeoutDelay[0];
			}).when(mockInstance).getLoadTimeoutDelay();
		} catch (Exception exception) {
		}
		return mockInstance;
	}

	final DataSegment segment1 = new DataSegment("test1", Intervals.of("2014/2015"), "v1", null, null, null, null, 0,
			0);

	final DataSegment segment2 = new DataSegment("test2", Intervals.of("2014/2015"), "v1", null, null, null, null, 0,
			0);

	final DataSegment segment3 = new DataSegment("test3", Intervals.of("2014/2015"), "v1", null, null, null, null, 0,
			0);

	final DataSegment segment4 = new DataSegment("test4", Intervals.of("2014/2015"), "v1", null, null, null, null, 0,
			0);

	final DruidCoordinatorConfig config = mockDruidCoordinatorConfig1(null, null, null, null, null, null, 10,
			Duration.ZERO);

	@Test(timeout = 60_000L)
	public void testSimple() throws Exception {
		HttpLoadQueuePeon httpLoadQueuePeon = new HttpLoadQueuePeon("http://dummy:4000", ServerTestHelper.MAPPER,
				new TestHttpClient(), config,
				Executors.newScheduledThreadPool(2, Execs.makeThreadFactory("HttpLoadQueuePeonTest-%s")),
				Execs.singleThreaded("HttpLoadQueuePeonTest"));

		httpLoadQueuePeon.start();

		Map<SegmentId, CountDownLatch> latches = ImmutableMap.of(segment1.getId(), new CountDownLatch(1),
				segment2.getId(), new CountDownLatch(1), segment3.getId(), new CountDownLatch(1), segment4.getId(),
				new CountDownLatch(1));

		httpLoadQueuePeon.dropSegment(segment1, () -> latches.get(segment1.getId()).countDown());
		httpLoadQueuePeon.loadSegment(segment2, () -> latches.get(segment2.getId()).countDown());
		httpLoadQueuePeon.dropSegment(segment3, () -> latches.get(segment3.getId()).countDown());
		httpLoadQueuePeon.loadSegment(segment4, () -> latches.get(segment4.getId()).countDown());

		latches.get(segment1.getId()).await();
		latches.get(segment2.getId()).await();
		latches.get(segment3.getId()).await();
		latches.get(segment4.getId()).await();

		httpLoadQueuePeon.stop();
	}

	@Test(timeout = 60_000L)
	public void testLoadDropAfterStop() throws Exception {
		HttpLoadQueuePeon httpLoadQueuePeon = new HttpLoadQueuePeon("http://dummy:4000", ServerTestHelper.MAPPER,
				new TestHttpClient(), config,
				Executors.newScheduledThreadPool(2, Execs.makeThreadFactory("HttpLoadQueuePeonTest-%s")),
				Execs.singleThreaded("HttpLoadQueuePeonTest"));

		httpLoadQueuePeon.start();

		Map<SegmentId, CountDownLatch> latches = ImmutableMap.of(segment1.getId(), new CountDownLatch(1),
				segment2.getId(), new CountDownLatch(1), segment3.getId(), new CountDownLatch(1), segment4.getId(),
				new CountDownLatch(1));

		httpLoadQueuePeon.dropSegment(segment1, () -> latches.get(segment1.getId()).countDown());
		httpLoadQueuePeon.loadSegment(segment2, () -> latches.get(segment2.getId()).countDown());
		latches.get(segment1.getId()).await();
		latches.get(segment2.getId()).await();
		httpLoadQueuePeon.stop();
		httpLoadQueuePeon.dropSegment(segment3, () -> latches.get(segment3.getId()).countDown());
		httpLoadQueuePeon.loadSegment(segment4, () -> latches.get(segment4.getId()).countDown());
		latches.get(segment3.getId()).await();
		latches.get(segment4.getId()).await();

	}

	private static class TestHttpClient implements HttpClient {
		@Override
		public <Intermediate, Final> ListenableFuture<Final> go(Request request,
				HttpResponseHandler<Intermediate, Final> httpResponseHandler) {
			throw new UnsupportedOperationException("Not Implemented.");
		}

		@Override
		public <Intermediate, Final> ListenableFuture<Final> go(Request request,
				HttpResponseHandler<Intermediate, Final> httpResponseHandler, Duration duration) {
			HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
			httpResponse.setContent(ChannelBuffers.buffer(0));
			httpResponseHandler.handleResponse(httpResponse, null);
			try {
				List<DataSegmentChangeRequest> changeRequests = ServerTestHelper.MAPPER
						.readValue(request.getContent().array(), new TypeReference<List<DataSegmentChangeRequest>>() {
						});

				List<SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus> statuses = new ArrayList<>(
						changeRequests.size());
				for (DataSegmentChangeRequest cr : changeRequests) {
					statuses.add(new SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus(cr,
							SegmentLoadDropHandler.Status.SUCCESS));
				}
				return (ListenableFuture) Futures.immediateFuture(new ByteArrayInputStream(ServerTestHelper.MAPPER
						.writerWithType(HttpLoadQueuePeon.RESPONSE_ENTITY_TYPE_REF).writeValueAsBytes(statuses)));
			} catch (Exception ex) {
				throw new RE(ex, "Unexpected exception.");
			}
		}
	}
}
