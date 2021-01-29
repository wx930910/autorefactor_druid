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

package org.apache.druid.java.util.metrics;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.cgroups.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

public class CpuAcctDeltaMonitorTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();
	private File procDir;
	private File cgroupDir;
	private File cpuacctDir;

	@Before
	public void setUp() throws IOException {
		cgroupDir = temporaryFolder.newFolder();
		procDir = temporaryFolder.newFolder();
		TestUtils.setUpCgroups(procDir, cgroupDir);
		cpuacctDir = new File(cgroupDir, "cpu,cpuacct/system.slice/some.service/f12ba7e0-fa16-462e-bb9d-652ccc27f0ee");
		Assert.assertTrue((cpuacctDir.isDirectory() && cpuacctDir.exists()) || cpuacctDir.mkdirs());
		TestUtils.copyResource("/cpuacct.usage_all", new File(cpuacctDir, "cpuacct.usage_all"));
	}

	@Test
	public void testMonitorWontCrash() {
		final CpuAcctDeltaMonitor monitor = new CpuAcctDeltaMonitor("some_feed", ImmutableMap.of(), cgroup -> {
			throw new RuntimeException("Should continue");
		});
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
		monitor.doMonitor(emitter);
		monitor.doMonitor(emitter);
		Assert.assertTrue(emitterEvents.isEmpty());
	}

}
