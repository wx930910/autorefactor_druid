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

package org.apache.druid.indexing.common.task.batch.parallel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.http.client.HttpClient;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import com.google.common.util.concurrent.Futures;

public class HttpShuffleClientTest {
	static public PartitionLocation<Integer> mockPartitionLocation1() {
		PartitionLocation<Integer> mockInstance = Mockito.mock(PartitionLocation.class,
				Mockito.withSettings().useConstructor(HOST, PORT, false, SUBTASK_ID, INTERVAL, PARTITION_ID)
						.defaultAnswer(Mockito.CALLS_REAL_METHODS));
		try {
			Mockito.doAnswer((stubInvo) -> {
				return (int) mockInstance.getSecondaryPartition();
			}).when(mockInstance).getBucketId();
		} catch (Exception exception) {
		}
		return mockInstance;
	}

	private static final String SUPERVISOR_TASK_ID = "supervisorTaskId";
	private static final String SUBTASK_ID = "subtaskId";
	private static final Interval INTERVAL = Intervals.of("2019/2020");
	private static final String HOST = "host";
	private static final int PORT = 1080;
	private static final int PARTITION_ID = 0;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private File segmentFile;

	@Before
	public void setup() throws IOException {
		segmentFile = temporaryFolder.newFile();
		try (Writer writer = Files.newBufferedWriter(segmentFile.toPath(), StandardCharsets.UTF_8)) {
			for (int j = 0; j < 10; j++) {
				writer.write(StringUtils.format("let's write some data.\n"));
			}
		}
	}

	@Test
	public void testFetchSegmentFileWithValidParamsReturningCopiedFileInPartitoinDir() throws IOException {
		ShuffleClient shuffleClient = mockClient(0);
		final File localDir = temporaryFolder.newFolder();
		final File fetchedFile = shuffleClient.fetchSegmentFile(localDir, SUPERVISOR_TASK_ID,
				HttpShuffleClientTest.mockPartitionLocation1());
		Assert.assertEquals(fetchedFile.getParentFile(), localDir);
	}

	@Test
	public void testFetchUnknownPartitionThrowingIOExceptionAfterRetries() throws IOException {
		expectedException.expect(IOException.class);
		ShuffleClient shuffleClient = mockClient(HttpShuffleClient.NUM_FETCH_RETRIES + 1);
		shuffleClient.fetchSegmentFile(temporaryFolder.newFolder(), SUPERVISOR_TASK_ID,
				HttpShuffleClientTest.mockPartitionLocation1());
	}

	@Test
	public void testFetchSegmentFileWithTransientFailuresReturningCopiedFileInPartitionDir() throws IOException {
		ShuffleClient shuffleClient = mockClient(HttpShuffleClient.NUM_FETCH_RETRIES - 1);
		final File localDir = temporaryFolder.newFolder();
		final File fetchedFile = shuffleClient.fetchSegmentFile(localDir, SUPERVISOR_TASK_ID,
				HttpShuffleClientTest.mockPartitionLocation1());
		Assert.assertEquals(fetchedFile.getParentFile(), localDir);
	}

	@Test
	public void testFetchSegmentFileWithTwoThreadsReturningCopiedFilesInPartitionDir()
			throws IOException, ExecutionException, InterruptedException {
		ExecutorService service = Execs.multiThreaded(2, "http-shuffle-client-test-%d");
		ShuffleClient shuffleClient = mockClient(0);
		try {
			List<Future<File>> futures = new ArrayList<>();
			List<File> localDirs = new ArrayList<>();
			for (int i = 0; i < 2; i++) {
				localDirs.add(temporaryFolder.newFolder());
			}
			for (int i = 0; i < 2; i++) {
				final File localDir = localDirs.get(i);
				futures.add(service.submit(() -> shuffleClient.fetchSegmentFile(localDir, SUPERVISOR_TASK_ID,
						HttpShuffleClientTest.mockPartitionLocation1())));
			}

			for (int i = 0; i < futures.size(); i++) {
				Assert.assertEquals(futures.get(i).get().getParentFile(), localDirs.get(i));
			}
		} finally {
			service.shutdownNow();
		}
	}

	@Test
	public void testFetchSegmentFileWithTwoThreadsAndTransitentFailuresReturningCopiedFilesInPartitionDir()
			throws IOException, ExecutionException, InterruptedException {
		ExecutorService service = Execs.multiThreaded(2, "http-shuffle-client-test-%d");
		ShuffleClient shuffleClient = mockClient(HttpShuffleClient.NUM_FETCH_RETRIES - 1);
		try {
			List<Future<File>> futures = new ArrayList<>();
			List<File> localDirs = new ArrayList<>();
			for (int i = 0; i < 2; i++) {
				localDirs.add(temporaryFolder.newFolder());
			}
			for (int i = 0; i < 2; i++) {
				final File localDir = localDirs.get(i);
				futures.add(service.submit(() -> shuffleClient.fetchSegmentFile(localDir, SUPERVISOR_TASK_ID,
						HttpShuffleClientTest.mockPartitionLocation1())));
			}

			for (int i = 0; i < futures.size(); i++) {
				Assert.assertEquals(futures.get(i).get().getParentFile(), localDirs.get(i));
			}
		} finally {
			service.shutdownNow();
		}
	}

	private HttpShuffleClient mockClient(int numFailures) throws FileNotFoundException {
		HttpClient httpClient = EasyMock.strictMock(HttpClient.class);
		if (numFailures == 0) {
			EasyMock.expect(httpClient.go(EasyMock.anyObject(), EasyMock.anyObject()))
					// should return different instances of input stream
					.andReturn(Futures.immediateFuture(new FileInputStream(segmentFile)))
					.andReturn(Futures.immediateFuture(new FileInputStream(segmentFile)));
		} else {
			EasyMock.expect(httpClient.go(EasyMock.anyObject(), EasyMock.anyObject()))
					.andReturn(Futures.immediateFailedFuture(new RuntimeException())).times(numFailures)
					// should return different instances of input stream
					.andReturn(Futures.immediateFuture(new FileInputStream(segmentFile)))
					.andReturn(Futures.immediateFuture(new FileInputStream(segmentFile)));
		}
		EasyMock.replay(httpClient);
		return new HttpShuffleClient(httpClient);
	}
}