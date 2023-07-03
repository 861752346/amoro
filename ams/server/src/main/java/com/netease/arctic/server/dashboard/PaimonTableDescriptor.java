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

package com.netease.arctic.server.dashboard;

import com.netease.arctic.ams.api.TableFormat;
import com.netease.arctic.server.dashboard.model.AMSColumnInfo;
import com.netease.arctic.server.dashboard.model.AMSDataFileInfo;
import com.netease.arctic.server.dashboard.model.AMSPartitionField;
import com.netease.arctic.server.dashboard.model.DDLInfo;
import com.netease.arctic.server.dashboard.model.PartitionBaseInfo;
import com.netease.arctic.server.dashboard.model.PartitionFileBaseInfo;
import com.netease.arctic.server.dashboard.model.ServerTableMeta;
import com.netease.arctic.server.dashboard.model.TransactionsOfTable;
import com.netease.arctic.table.ATable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.paimon.table.Table;

public class PaimonTableDescriptor implements FormatTableDescriptor{
  @Override
  public List<TableFormat> supportFormat() {
    return Arrays.asList(TableFormat.PAIMON);
  }

  @Override
  public ServerTableMeta getTableDetail(ATable<?> aTable) {
    Table table = getTable(aTable);

    ServerTableMeta serverTableMeta = new ServerTableMeta();
    serverTableMeta.setTableIdentifier(aTable.id());
    serverTableMeta.setTableType(TableFormat.PAIMON.name());

    //schema
    serverTableMeta.setSchema(
        table.rowType().getFields().stream()
            .map(s -> new AMSColumnInfo(s.name(), s.type().asSQLString(), s.description()))
            .collect(Collectors.toList())
    );

    //pk
    Set<String> primaryKeyNames = new HashSet<>(table.primaryKeys());
    List<AMSColumnInfo> primaryKeys = serverTableMeta.getSchema()
        .stream()
        .filter(s -> primaryKeyNames.contains(s.getField()))
        .collect(Collectors.toList());
    serverTableMeta.setPkList(primaryKeys);

    //partition
    List<AMSPartitionField> partitionFields = table.partitionKeys().stream()
        .map(partition -> new AMSPartitionField(partition, null, null, null, null))
        .collect(Collectors.toList());
    serverTableMeta.setPartitionColumnList(partitionFields);

    //properties
    serverTableMeta.setProperties(table.options());


    return serverTableMeta;
  }

  @Override
  public List<TransactionsOfTable> getTransactions(ATable<?> aTable) {
    Table table = getTable(aTable);
    return null;
  }

  @Override
  public List<AMSDataFileInfo> getTransactionDetail(ATable<?> aTable, long transactionId) {
    return null;
  }

  @Override
  public List<DDLInfo> getTableOperations(ATable<?> aTable) {
    return null;
  }

  @Override
  public List<PartitionBaseInfo> getTablePartition(ATable<?> aTable) {
    return null;
  }

  @Override
  public List<PartitionFileBaseInfo> getTableFile(ATable<?> aTable, String partition, int limit) {
    return null;
  }

  private Table getTable(ATable<?> aTable) {
    return (Table)aTable.originalTable();
  }
}
