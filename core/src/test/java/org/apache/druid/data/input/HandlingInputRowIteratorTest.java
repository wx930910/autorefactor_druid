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

package org.apache.druid.data.input;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.druid.data.input.HandlingInputRowIterator.InputRowHandler;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

@RunWith(Enclosed.class)
public class HandlingInputRowIteratorTest {

	public static class PresentRowTest {
		boolean unsuccessfulHandlerSuccessful;

		private static final InputRow INPUT_ROW1 = EasyMock.mock(InputRow.class);
		private static final InputRow INPUT_ROW2 = EasyMock.mock(InputRow.class);
		private static final List<InputRow> INPUT_ROWS = Arrays.asList(INPUT_ROW1, INPUT_ROW2);

		private InputRowHandler unsuccessfulHandler;

		@Before
		public void setup() {
			unsuccessfulHandler = Mockito.mock(HandlingInputRowIterator.InputRowHandler.class);
			unsuccessfulHandlerSuccessful = false;
			try {
				Mockito.when(unsuccessfulHandler.handle(Mockito.any())).thenAnswer((stubInvo) -> {
					return unsuccessfulHandlerSuccessful;
				});
			} catch (Exception exception) {
				exception.printStackTrace();
			}
		}

		@Test
		public void hasNext() {
			HandlingInputRowIterator target = createInputRowIterator(unsuccessfulHandler, unsuccessfulHandler);
			Assert.assertTrue(target.hasNext());
			Mockito.verify(unsuccessfulHandler, Mockito.never()).handle(Mockito.any());
		}

		private static HandlingInputRowIterator createInputRowIterator(
				HandlingInputRowIterator.InputRowHandler firstHandler,
				HandlingInputRowIterator.InputRowHandler secondHandler) {
			CloseableIterator<InputRow> iterator = CloseableIterators.withEmptyBaggage(new Iterator<InputRow>() {
				private final Iterator<InputRow> delegate = INPUT_ROWS.iterator();

				@Override
				public boolean hasNext() {
					return delegate.hasNext();
				}

				@Nullable
				@Override
				public InputRow next() {
					return delegate.next();
				}
			});

			return new HandlingInputRowIterator(iterator, Arrays.asList(firstHandler, secondHandler));
		}
	}
}
