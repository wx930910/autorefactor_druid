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

package org.apache.druid.indexing.overlord;

import org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.joda.time.Period;
import org.mockito.Mockito;

/**
 */
public class TestRemoteTaskRunnerConfig {
	static public RemoteTaskRunnerConfig mockRemoteTaskRunnerConfig1(Period timeout) {
		Period[] mockFieldVariableTimeout = new Period[1];
		RemoteTaskRunnerConfig mockInstance = Mockito.spy(RemoteTaskRunnerConfig.class);
		mockFieldVariableTimeout[0] = timeout;
		try {
			Mockito.doAnswer((stubInvo) -> {
				return mockFieldVariableTimeout[0];
			}).when(mockInstance).getTaskAssignmentTimeout();
			Mockito.doAnswer((stubInvo) -> {
				return "";
			}).when(mockInstance).getMinWorkerVersion();
			Mockito.doAnswer((stubInvo) -> {
				return mockFieldVariableTimeout[0];
			}).when(mockInstance).getWorkerBlackListCleanupPeriod();
			Mockito.doAnswer((stubInvo) -> {
				return mockFieldVariableTimeout[0];
			}).when(mockInstance).getWorkerBlackListBackoffTime();
			Mockito.doAnswer((stubInvo) -> {
				return 1;
			}).when(mockInstance).getMaxRetriesBeforeBlacklist();
			Mockito.doAnswer((stubInvo) -> {
				return mockFieldVariableTimeout[0];
			}).when(mockInstance).getTaskCleanupTimeout();
			Mockito.doAnswer((stubInvo) -> {
				return mockFieldVariableTimeout[0];
			}).when(mockInstance).getTaskShutdownLinkTimeout();
			Mockito.doAnswer((stubInvo) -> {
				return 10 * 1024;
			}).when(mockInstance).getMaxZnodeBytes();
		} catch (Exception exception) {
		}
		return mockInstance;
	}
}
