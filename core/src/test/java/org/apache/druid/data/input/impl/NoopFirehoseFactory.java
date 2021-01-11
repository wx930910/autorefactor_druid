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

package org.apache.druid.data.input.impl;

import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.mockito.Mockito;

public class NoopFirehoseFactory {
	static public FiniteFirehoseFactory mockFiniteFirehoseFactory1() {
		FiniteFirehoseFactory mockInstance = Mockito.spy(FiniteFirehoseFactory.class);
		try {
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).getNumSplits(Mockito.any(SplitHintSpec.class));
			Mockito.doAnswer((stubInvo) -> {
				return "NoopFirehoseFactory{}";
			}).when(mockInstance).toString();
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).withSplit(Mockito.any(InputSplit.class));
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).getSplits(Mockito.any(SplitHintSpec.class));
		} catch (Exception exception) {
		}
		return mockInstance;
	}
}
