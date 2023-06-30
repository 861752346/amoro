/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.table;

import com.netease.arctic.ams.api.TableFormat;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Table;

public class MixedTable implements ATable<ArcticTable>{

  private ArcticTable arcticTable;

  public MixedTable(ArcticTable arcticTable) {
    this.arcticTable = arcticTable;
  }

  @Override
  public TableIdentifier id() {
    return arcticTable.id();
  }

  @Override
  public TableFormat format() {
    return arcticTable.format();
  }

  @Override
  public String name() {
    return arcticTable.name();
  }

  @Override
  public Map<String, String> properties() {
    return arcticTable.properties();
  }

  @Override
  public Snapshot currentSnapshot() {
    MixedSnapshot mixedSnapshot;
    if (arcticTable.isKeyedTable()) {
      KeyedTable table = arcticTable.asKeyedTable();
      mixedSnapshot = new MixedSnapshot(
          Optional.ofNullable(table.changeTable())
              .map(Table::currentSnapshot)
              .map(org.apache.iceberg.Snapshot::snapshotId).orElse(null),
          Optional.ofNullable(table.baseTable())
              .map(Table::currentSnapshot)
              .map(org.apache.iceberg.Snapshot::snapshotId).orElse(null));
    } else {
      mixedSnapshot = new MixedSnapshot(null,
          Optional.ofNullable(arcticTable.asUnkeyedTable())
          .map(Table::currentSnapshot)
          .map(org.apache.iceberg.Snapshot::snapshotId).orElse(null));
    }
    return mixedSnapshot;
  }

  @Override
  public ArcticTable originalTable() {
    return arcticTable;
  }

  public static class MixedSnapshot implements Snapshot {

    private Long changeSnapshot;

    private Long baseSnapshot;

    public MixedSnapshot(Long changeSnapshot, Long baseSnapshot) {
      this.changeSnapshot = changeSnapshot == null ? -1L : changeSnapshot;
      this.baseSnapshot = baseSnapshot == null ? -1L : baseSnapshot;
    }

    @Override
    public boolean equals(Snapshot snapshot) {
      if (snapshot.equals(Snapshot.invalid())) {
        return false;
      }
      if (!(snapshot instanceof MixedSnapshot)) {
        throw new RuntimeException();
      }
      MixedSnapshot mixedSnapshot = (MixedSnapshot) snapshot;
      return changeSnapshot == mixedSnapshot.changeSnapshot && baseSnapshot == mixedSnapshot.baseSnapshot;
    }

    public Long getChangeSnapshot() {
      return changeSnapshot;
    }

    public Long getBaseSnapshot() {
      return baseSnapshot;
    }
  }
}
