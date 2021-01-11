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

package org.apache.druid.curator.inventory;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;

/**
 */
public class CuratorInventoryManagerTest extends CuratorTestBase {
	static public InventoryManagerConfig mockInventoryManagerConfig1(String containerPath, String inventoryPath) {
		String[] mockFieldVariableInventoryPath = new String[1];
		String[] mockFieldVariableContainerPath = new String[1];
		InventoryManagerConfig mockInstance = Mockito.spy(InventoryManagerConfig.class);
		mockFieldVariableContainerPath[0] = containerPath;
		mockFieldVariableInventoryPath[0] = inventoryPath;
		try {
			Mockito.doAnswer((stubInvo) -> {
				return mockFieldVariableInventoryPath[0];
			}).when(mockInstance).getInventoryPath();
			Mockito.doAnswer((stubInvo) -> {
				return mockFieldVariableContainerPath[0];
			}).when(mockInstance).getContainerPath();
		} catch (Exception exception) {
		}
		return mockInstance;
	}

	private ExecutorService exec;

	@Before
	public void setUp() throws Exception {
		setupServerAndCurator();
		exec = Execs.singleThreaded("curator-inventory-manager-test-%s");
	}

	@After
	public void tearDown() {
		tearDownServerAndCurator();
	}

	@Test
	public void testSanity() throws Exception {
		final CuratorInventoryManagerStrategy<Map<String, Integer>, Integer> strategy = Mockito
				.spy(CuratorInventoryManagerStrategy.class);
		CountDownLatch[] strategyNewContainerLatch = new CountDownLatch[] { null };
		CountDownLatch[] strategyDeadContainerLatch = new CountDownLatch[] { null };
		CountDownLatch[] strategyNewInventoryLatch = new CountDownLatch[] { null };
		CountDownLatch[] strategyDeadInventoryLatch = new CountDownLatch[] { null };
		try {
			Mockito.doAnswer((stubInvo) -> {
				return new TreeMap<>();
			}).when(strategy).deserializeContainer(Mockito.any(byte[].class));
			Mockito.doAnswer((stubInvo) -> {
				if (strategyNewContainerLatch[0] != null) {
					strategyNewContainerLatch[0].countDown();
				}
				return null;
			}).when(strategy).newContainer(Mockito.any(Map.class));
			Mockito.doAnswer((stubInvo) -> {
				Map<String, Integer> oldContainer = stubInvo.getArgument(0);
				Map<String, Integer> newContainer = stubInvo.getArgument(1);
				newContainer.putAll(oldContainer);
				return newContainer;
			}).when(strategy).updateContainer(Mockito.any(Map.class), Mockito.any(Map.class));
			Mockito.doAnswer((stubInvo) -> {
				if (strategyDeadContainerLatch[0] != null) {
					strategyDeadContainerLatch[0].countDown();
				}
				return null;
			}).when(strategy).deadContainer(Mockito.any(Map.class));
			Mockito.doAnswer((stubInvo) -> {
				return null;
			}).when(strategy).inventoryInitialized();
			Mockito.doAnswer((stubInvo) -> {
				Map<String, Integer> container = stubInvo.getArgument(0);
				String inventoryKey = stubInvo.getArgument(1);
				Integer inventoryMockVariable = stubInvo.getArgument(2);
				container.put(inventoryKey, inventoryMockVariable);
				if (strategyNewInventoryLatch[0] != null) {
					strategyNewInventoryLatch[0].countDown();
				}
				return container;
			}).when(strategy).addInventory(Mockito.any(Map.class), Mockito.any(String.class),
					Mockito.any(Integer.class));
			Mockito.doAnswer((stubInvo) -> {
				Map<String, Integer> container = stubInvo.getArgument(0);
				String inventoryKey = stubInvo.getArgument(1);
				Integer inventoryMockVariable = stubInvo.getArgument(2);
				return strategy.addInventory(container, inventoryKey, inventoryMockVariable);
			}).when(strategy).updateInventory(Mockito.any(Map.class), Mockito.any(String.class),
					Mockito.any(Integer.class));
			Mockito.doAnswer((stubInvo) -> {
				byte[] bytes = stubInvo.getArgument(0);
				return (Integer) Ints.fromByteArray(bytes);
			}).when(strategy).deserializeInventory(Mockito.any(byte[].class));
			Mockito.doAnswer((stubInvo) -> {
				Map<String, Integer> container = stubInvo.getArgument(0);
				String inventoryKey = stubInvo.getArgument(1);
				container.remove(inventoryKey);
				if (strategyDeadInventoryLatch[0] != null) {
					strategyDeadInventoryLatch[0].countDown();
				}
				return container;
			}).when(strategy).removeInventory(Mockito.any(Map.class), Mockito.any(String.class));
		} catch (Exception exception) {
		}
		CuratorInventoryManager<Map<String, Integer>, Integer> manager = new CuratorInventoryManager<>(curator,
				CuratorInventoryManagerTest.mockInventoryManagerConfig1("/container", "/inventory"), exec, strategy);

		curator.start();
		curator.blockUntilConnected();

		manager.start();

		Assert.assertTrue(Iterables.isEmpty(manager.getInventory()));

		CountDownLatch containerLatch = new CountDownLatch(1);
		strategyNewContainerLatch[0] = containerLatch;
		curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/container/billy",
				new byte[] {});

		Assert.assertTrue(timing.awaitLatch(containerLatch));
		strategyNewContainerLatch[0] = null;

		final Iterable<Map<String, Integer>> inventory = manager.getInventory();
		Assert.assertTrue(Iterables.getOnlyElement(inventory).isEmpty());

		CountDownLatch inventoryLatch = new CountDownLatch(2);
		strategyNewInventoryLatch[0] = inventoryLatch;
		curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/inventory/billy/1",
				Ints.toByteArray(100));
		curator.create().withMode(CreateMode.EPHEMERAL).forPath("/inventory/billy/bob", Ints.toByteArray(2287));

		Assert.assertTrue(timing.awaitLatch(inventoryLatch));
		strategyNewInventoryLatch[0] = null;

		verifyInventory(manager);

		CountDownLatch deleteLatch = new CountDownLatch(1);
		strategyDeadInventoryLatch[0] = deleteLatch;
		curator.delete().forPath("/inventory/billy/1");

		Assert.assertTrue(timing.awaitLatch(deleteLatch));
		strategyDeadInventoryLatch[0] = null;

		Assert.assertEquals(1, manager.getInventoryValue("billy").size());
		Assert.assertEquals(2287, manager.getInventoryValue("billy").get("bob").intValue());

		inventoryLatch = new CountDownLatch(1);
		strategyNewInventoryLatch[0] = inventoryLatch;
		curator.create().withMode(CreateMode.EPHEMERAL).forPath("/inventory/billy/1", Ints.toByteArray(100));

		Assert.assertTrue(timing.awaitLatch(inventoryLatch));
		strategyNewInventoryLatch[0] = null;

		verifyInventory(manager);

		final CountDownLatch latch = new CountDownLatch(1);
		curator.getCuratorListenable().addListener(new CuratorListener() {
			@Override
			public void eventReceived(CuratorFramework client, CuratorEvent event) {
				if (event.getType() == CuratorEventType.WATCHED
						&& event.getWatchedEvent().getState() == Watcher.Event.KeeperState.Disconnected) {
					latch.countDown();
				}
			}
		});

		server.stop();
		Assert.assertTrue(timing.awaitLatch(latch));

		verifyInventory(manager);

		Thread.sleep(50); // Wait a bit

		verifyInventory(manager);
	}

	private void verifyInventory(CuratorInventoryManager<Map<String, Integer>, Integer> manager) {
		final Map<String, Integer> vals = manager.getInventoryValue("billy");
		Assert.assertEquals(2, vals.size());
		Assert.assertEquals(100, vals.get("1").intValue());
		Assert.assertEquals(2287, vals.get("bob").intValue());
	}
}
