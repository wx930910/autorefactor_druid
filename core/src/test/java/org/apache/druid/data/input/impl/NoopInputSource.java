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

import java.io.File;

import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.mockito.Mockito;

public class NoopInputSource {
	static public InputSource mockInputSource1() {
		InputSource mockInstance = Mockito.spy(InputSource.class);
		try {
			Mockito.doAnswer((stubInvo) -> {
				return "NoopInputSource{}";
			}).when(mockInstance).toString();
			Mockito.doAnswer((stubInvo) -> {
				return false;
			}).when(mockInstance).needsFormat();
			Mockito.doAnswer((stubInvo) -> {
				return false;
			}).when(mockInstance).isSplittable();
			Mockito.doAnswer((stubInvo) -> {
				throw new UnsupportedOperationException();
			}).when(mockInstance).reader(Mockito.any(InputRowSchema.class), Mockito.any(InputFormat.class),
					Mockito.any(File.class));
		} catch (Exception exception) {
		}
		return mockInstance;
	}
}
