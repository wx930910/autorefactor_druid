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

package org.apache.druid.server.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

public class TaskCountStatsMonitorTest {
	private TaskCountStatsProvider statsProvider;

	@Before
	public void setUp() {
		statsProvider = new TaskCountStatsProvider() {
			@Override
			public Map<String, Long> getSuccessfulTaskCount() {
				return ImmutableMap.of("d1", 1L);
			}

			@Override
			public Map<String, Long> getFailedTaskCount() {
				return ImmutableMap.of("d1", 1L);
			}

			@Override
			public Map<String, Long> getRunningTaskCount() {
				return ImmutableMap.of("d1", 1L);
			}

			@Override
			public Map<String, Long> getPendingTaskCount() {
				return ImmutableMap.of("d1", 1L);
			}

			@Override
			public Map<String, Long> getWaitingTaskCount() {
				return ImmutableMap.of("d1", 1L);
			}
		};
	}

	@Test
	public void testMonitor() {
		final TaskCountStatsMonitor monitor = new TaskCountStatsMonitor(statsProvider);
		final ServiceEmitter emitter = Mockito.spy(new ServiceEmitter("service", "host", null));
		List<Event> emitterEvents = new ArrayList<>();
		try {
			Mockito.doNothing().when(emitter).flush();
			Mockito.doNothing().when(emitter).close();
			Mockito.doAnswer((stubInvo) -> {
				Event event = stubInvo.getArgument(0);
				emitterEvents.add(event);
				return null;
			}).when(emitter).emit(Mockito.any(Event.class));
			Mockito.doNothing().when(emitter).start();
		} catch (Exception exception) {
			exception.printStackTrace();
		}
		monitor.doMonitor(emitter);
		Assert.assertEquals(5, emitterEvents.size());
		Assert.assertEquals("task/success/count", emitterEvents.get(0).toMap().get("metric"));
		Assert.assertEquals(1L, emitterEvents.get(0).toMap().get("value"));
		Assert.assertEquals("task/failed/count", emitterEvents.get(1).toMap().get("metric"));
		Assert.assertEquals(1L, emitterEvents.get(1).toMap().get("value"));
		Assert.assertEquals("task/running/count", emitterEvents.get(2).toMap().get("metric"));
		Assert.assertEquals(1L, emitterEvents.get(2).toMap().get("value"));
		Assert.assertEquals("task/pending/count", emitterEvents.get(3).toMap().get("metric"));
		Assert.assertEquals(1L, emitterEvents.get(3).toMap().get("value"));
		Assert.assertEquals("task/waiting/count", emitterEvents.get(4).toMap().get("metric"));
		Assert.assertEquals(1L, emitterEvents.get(4).toMap().get("value"));
	}
}
