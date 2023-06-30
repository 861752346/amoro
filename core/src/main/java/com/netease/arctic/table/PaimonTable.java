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
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;

public class PaimonTable implements ATable<Table>{

  private DataTable table;

  private TableIdentifier tableIdentifier;

  public PaimonTable(Table table, TableIdentifier tableIdentifier) {
    this.table = (DataTable) table;
    this.tableIdentifier = tableIdentifier;
  }

  @Override
  public TableIdentifier id() {
    return tableIdentifier;
  }

  @Override
  public TableFormat format() {
    return TableFormat.PAIMON;
  }

  @Override
  public String name() {
    return table.name();
  }

  @Override
  public Map<String, String> properties() {
    return table.options();
  }

  @Override
  public Snapshot currentSnapshot() {
    return new PaimonSnapshot(table.snapshotManager().latestSnapshot());
  }

  @Override
  public Table originalTable() {
    return table;
  }

  public static class PaimonSnapshot implements Snapshot {

    private org.apache.paimon.Snapshot snapshot;

    public PaimonSnapshot(org.apache.paimon.Snapshot snapshot) {
      this.snapshot = snapshot;
    }

    @Override
    public boolean equals(Snapshot snapshot) {
      if (Snapshot.invalid().equals(snapshot)) {
        return false;
      }
      if (!(snapshot instanceof PaimonSnapshot)) {
        throw new RuntimeException();
      }
      PaimonSnapshot paimonSnapshot = (PaimonSnapshot) snapshot;
      return this.snapshot.equals(paimonSnapshot.snapshot);
    }
  }
}
