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

import org.joda.time.Duration;
import org.mockito.Mockito;

public class TestDruidCoordinatorConfig {

	static public DruidCoordinatorConfig mockDruidCoordinatorConfig1(Duration coordinatorStartDelay,
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
}
