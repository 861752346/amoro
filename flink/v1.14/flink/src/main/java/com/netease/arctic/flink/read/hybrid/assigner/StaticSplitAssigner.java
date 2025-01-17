/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.flink.read.hybrid.assigner;

import com.netease.arctic.flink.read.hybrid.enumerator.ArcticSourceEnumState;
import com.netease.arctic.flink.read.hybrid.split.ArcticSplit;
import com.netease.arctic.flink.read.hybrid.split.ArcticSplitState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This is a static split assigner which is used for batch mode.
 */
public class StaticSplitAssigner implements SplitAssigner {
  private static final Logger LOG = LoggerFactory.getLogger(StaticSplitAssigner.class);

  private static final long POLL_TIMEOUT = 200;
  private int totalSplitNum;

  private final PriorityBlockingQueue<ArcticSplit> splitQueue;

  private CompletableFuture<Void> availableFuture;

  public StaticSplitAssigner(@Nullable ArcticSourceEnumState enumState) {
    this.splitQueue = new PriorityBlockingQueue<>();
    if (enumState != null) {
      Collection<ArcticSplitState> splitStates = enumState.pendingSplits();
      splitStates.forEach(state -> onDiscoveredSplits(Collections.singleton(state.toSourceSplit())));
    }
  }

  @Override
  public Split getNext() {
    return getNextSplit().map(Split::of).orElseGet(Split::unavailable);
  }

  @Override
  public Split getNext(int subtaskId) {
    return getNext();
  }

  private Optional<ArcticSplit> getNextSplit() {
    ArcticSplit arcticSplit = null;
    try {
      arcticSplit = splitQueue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted when polling splits from the split queue", e);
    }
    if (arcticSplit == null) {
      LOG.debug("Couldn't retrieve arctic source split from the queue, as the queue is empty.");
      return Optional.empty();
    } else {
      LOG.info("Assigning the arctic split, task index is {}, total number of splits is {}, arctic split is {}.",
          arcticSplit.taskIndex(), totalSplitNum, arcticSplit);
      return Optional.of(arcticSplit);
    }
  }

  @Override
  public void onDiscoveredSplits(Collection<ArcticSplit> splits) {
    splits.forEach(this::putArcticIntoQueue);
    totalSplitNum += splits.size();
    // only complete pending future if new splits are discovered
    completeAvailableFuturesIfNeeded();
  }

  @Override
  public void onUnassignedSplits(Collection<ArcticSplit> splits) {
    onDiscoveredSplits(splits);
  }

  void putArcticIntoQueue(final ArcticSplit split) {
    splitQueue.put(split);
  }

  @Override
  public Collection<ArcticSplitState> state() {
    return splitQueue.stream().map(ArcticSplitState::new).collect(Collectors.toList());
  }

  @Override
  public synchronized CompletableFuture<Void> isAvailable() {
    if (availableFuture == null) {
      availableFuture = new CompletableFuture<>();
    }
    return availableFuture;
  }

  public boolean isEmpty() {
    return splitQueue.isEmpty();
  }

  @Override
  public void close() throws IOException {
    splitQueue.clear();
  }

  private synchronized void completeAvailableFuturesIfNeeded() {
    if (availableFuture != null && !isEmpty()) {
      availableFuture.complete(null);
    }
    availableFuture = null;
  }
}
