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

import com.netease.arctic.flink.read.FlinkSplitPlanner;
import com.netease.arctic.flink.read.hybrid.reader.TestRowDataReaderFunction;
import com.netease.arctic.flink.read.hybrid.split.ArcticSplit;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TestStaticSplitAssigner extends TestRowDataReaderFunction {
  private static final Logger LOG = LoggerFactory.getLogger(TestStaticSplitAssigner.class);

  @Test
  public void testSingleParallelism() throws IOException {
    try (StaticSplitAssigner staticSplitAssigner = instanceStaticSplitAssigner()) {
      List<ArcticSplit> splitList = FlinkSplitPlanner.mergeOnReadPlan(testKeyedTable, Collections.emptyList(),
          new AtomicInteger());
      staticSplitAssigner.onDiscoveredSplits(splitList);
      List<ArcticSplit> actual = new ArrayList<>();

      while (true) {
        Split splitOpt = staticSplitAssigner.getNext(0);
        if (splitOpt.isAvailable()) {
          actual.add(splitOpt.split());
        } else {
          break;
        }
      }

      Assert.assertEquals(splitList.size(), actual.size());
    }
  }

  @Test
  public void testMultiParallelism() throws IOException {
    try (StaticSplitAssigner staticSplitAssigner = instanceStaticSplitAssigner()) {
      List<ArcticSplit> splitList = FlinkSplitPlanner.mergeOnReadPlan(testKeyedTable, Collections.emptyList(),
          new AtomicInteger());
      staticSplitAssigner.onDiscoveredSplits(splitList);
      List<ArcticSplit> actual = new ArrayList<>();

      int subtaskId = 2;
      while (subtaskId >= 0) {
        Split splitOpt = staticSplitAssigner.getNext(subtaskId);
        if (splitOpt.isAvailable()) {
          actual.add(splitOpt.split());
        } else {
          LOG.info("Subtask id {}, splits {}.\n {}", subtaskId, actual.size(), actual);
          --subtaskId;
        }
      }

      Assert.assertEquals(splitList.size(), actual.size());
    }
  }

  protected StaticSplitAssigner instanceStaticSplitAssigner() {
    return new StaticSplitAssigner(null);
  }
}
