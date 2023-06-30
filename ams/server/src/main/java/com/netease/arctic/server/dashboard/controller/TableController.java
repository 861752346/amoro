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

package com.netease.arctic.server.dashboard.controller;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netease.arctic.ams.api.CatalogMeta;
import com.netease.arctic.ams.api.TableFormat;
import com.netease.arctic.catalog.CatalogLoader;
import com.netease.arctic.hive.HiveTableProperties;
import com.netease.arctic.hive.catalog.ArcticHiveCatalog;
import com.netease.arctic.hive.utils.HiveTableUtil;
import com.netease.arctic.hive.utils.UpgradeHiveTableUtil;
import com.netease.arctic.server.catalog.IcebergCatalogImpl;
import com.netease.arctic.server.catalog.MixedHiveCatalogImpl;
import com.netease.arctic.server.catalog.ServerCatalog;
import com.netease.arctic.server.dashboard.ServerTableDescriptor;
import com.netease.arctic.server.dashboard.ServerTableProperties;
import com.netease.arctic.server.dashboard.model.AMSColumnInfo;
import com.netease.arctic.server.dashboard.model.AMSDataFileInfo;
import com.netease.arctic.server.dashboard.model.AMSPartitionField;
import com.netease.arctic.server.dashboard.model.AMSTransactionsOfTable;
import com.netease.arctic.server.dashboard.model.DDLInfo;
import com.netease.arctic.server.dashboard.model.FilesStatistics;
import com.netease.arctic.server.dashboard.model.HiveTableInfo;
import com.netease.arctic.server.dashboard.model.OptimizedRecord;
import com.netease.arctic.server.dashboard.model.PartitionBaseInfo;
import com.netease.arctic.server.dashboard.model.PartitionFileBaseInfo;
import com.netease.arctic.server.dashboard.model.ServerTableMeta;
import com.netease.arctic.server.dashboard.model.TableBasicInfo;
import com.netease.arctic.server.dashboard.model.TableMeta;
import com.netease.arctic.server.dashboard.model.TableOperation;
import com.netease.arctic.server.dashboard.model.TableStatistics;
import com.netease.arctic.server.dashboard.model.TransactionsOfTable;
import com.netease.arctic.server.dashboard.model.UpgradeHiveMeta;
import com.netease.arctic.server.dashboard.model.UpgradeRunningInfo;
import com.netease.arctic.server.dashboard.model.UpgradeStatus;
import com.netease.arctic.server.dashboard.response.OkResponse;
import com.netease.arctic.server.dashboard.response.PageResult;
import com.netease.arctic.server.dashboard.utils.AmsUtil;
import com.netease.arctic.server.dashboard.utils.CommonUtil;
import com.netease.arctic.server.dashboard.utils.TableStatCollector;
import com.netease.arctic.server.table.ServerTableIdentifier;
import com.netease.arctic.server.table.TableService;
import com.netease.arctic.server.utils.Configurations;
import com.netease.arctic.table.ATable;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.KeyedTable;
import com.netease.arctic.table.PrimaryKeySpec;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.table.UnkeyedTable;
import io.javalin.http.Context;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Table moudle controller.
 */
public class TableController {
  private static final Logger LOG = LoggerFactory.getLogger(TableController.class);
  private static final long UPGRADE_INFO_EXPIRE_INTERVAL = 60 * 60 * 1000;

  private final TableService tableService;
  private final ServerTableDescriptor tableDescriptor;
  private final Configurations serviceConfig;
  private final ConcurrentHashMap<TableIdentifier, UpgradeRunningInfo> upgradeRunningInfo = new ConcurrentHashMap<>();
  private final ScheduledExecutorService tableUpgradeExecutor;

  public TableController(
      TableService tableService,
      ServerTableDescriptor tableDescriptor,
      Configurations serviceConfig) {
    this.tableService = tableService;
    this.tableDescriptor = tableDescriptor;
    this.serviceConfig = serviceConfig;
    this.tableUpgradeExecutor = Executors.newScheduledThreadPool(
        0,
        new ThreadFactoryBuilder()
            .setDaemon(false)
            .setNameFormat("ASYNC-HIVE-TABLE-UPGRADE-%d").build());
  }

  /**
   * getRuntime table detail.
   */
  public void getTableDetail(Context ctx) {

    String catalog = ctx.pathParam("catalog");
    String database = ctx.pathParam("db");
    String tableMame = ctx.pathParam("table");

    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog) && StringUtils.isNotBlank(database) && StringUtils.isNotBlank(tableMame),
        "catalog.database.tableName can not be empty in any element");
    Preconditions.checkState(tableService.catalogExist(catalog), "invalid catalog!");
    ServerTableMeta serverTableMeta =
        tableDescriptor.getTableDetail(ServerTableIdentifier.of(catalog, database, tableMame));
    ctx.json(OkResponse.of(serverTableMeta));
  }

  /**
   * getRuntime hive table detail.
   */
  public void getHiveTableDetail(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog) && StringUtils.isNotBlank(db) && StringUtils.isNotBlank(table),
        "catalog.database.tableName can not be empty in any element");
    Preconditions.checkArgument(
        tableService.getServerCatalog(catalog) instanceof MixedHiveCatalogImpl,
        "catalog {} is not a mixed hive catalog, so not support load hive tables", catalog);

    // getRuntime table from catalog
    MixedHiveCatalogImpl arcticHiveCatalog = (MixedHiveCatalogImpl) tableService.getServerCatalog(catalog);

    TableIdentifier tableIdentifier = TableIdentifier.of(catalog, db, table);
    HiveTableInfo hiveTableInfo;
    Table hiveTable = HiveTableUtil.loadHmsTable(arcticHiveCatalog.getHiveClient(), tableIdentifier);
    List<AMSColumnInfo> schema = transformHiveSchemaToAMSColumnInfo(hiveTable.getSd().getCols());
    List<AMSColumnInfo> partitionColumnInfos = transformHiveSchemaToAMSColumnInfo(hiveTable.getPartitionKeys());
    hiveTableInfo = new HiveTableInfo(tableIdentifier, TableMeta.TableType.HIVE, schema, partitionColumnInfos,
        new HashMap<>(), hiveTable.getCreateTime());
    ctx.json(OkResponse.of(hiveTableInfo));
  }

  /**
   * upgrade hive table to arctic.
   */
  public void upgradeHiveTable(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog) && StringUtils.isNotBlank(db) && StringUtils.isNotBlank(table),
        "catalog.database.tableName can not be empty in any element");
    UpgradeHiveMeta upgradeHiveMeta = ctx.bodyAsClass(UpgradeHiveMeta.class);

    ArcticHiveCatalog arcticHiveCatalog
        = (ArcticHiveCatalog) CatalogLoader.load(String.join("/", AmsUtil.getAMSThriftAddress(serviceConfig), catalog));

    tableUpgradeExecutor.execute(() -> {
      TableIdentifier tableIdentifier = TableIdentifier.of(catalog, db, table);
      upgradeRunningInfo.put(tableIdentifier, new UpgradeRunningInfo());
      try {
        UpgradeHiveTableUtil.upgradeHiveTable(arcticHiveCatalog, TableIdentifier.of(catalog, db, table),
            upgradeHiveMeta.getPkList()
                .stream()
                .map(UpgradeHiveMeta.PrimaryKeyField::getFieldName)
                .collect(Collectors.toList()), upgradeHiveMeta.getProperties());
        upgradeRunningInfo.get(tableIdentifier).setStatus(UpgradeStatus.SUCCESS.toString());
      } catch (Throwable t) {
        LOG.error("Failed to upgrade hive table to arctic ", t);
        upgradeRunningInfo.get(tableIdentifier).setErrorMessage(AmsUtil.getStackTrace(t));
        upgradeRunningInfo.get(tableIdentifier).setStatus(UpgradeStatus.FAILED.toString());
      } finally {
        tableUpgradeExecutor.schedule(
            () -> upgradeRunningInfo.remove(tableIdentifier),
            UPGRADE_INFO_EXPIRE_INTERVAL,
            TimeUnit.MILLISECONDS);
      }
    });
    ctx.json(OkResponse.ok());
  }

  public void getUpgradeStatus(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    UpgradeRunningInfo info = upgradeRunningInfo.containsKey(TableIdentifier.of(catalog, db, table)) ?
        upgradeRunningInfo.get(TableIdentifier.of(catalog, db, table)) :
        new UpgradeRunningInfo(UpgradeStatus.NONE.toString());
    ctx.json(OkResponse.of(info));
  }

  /**
   * upgrade hive table to arctic.
   */
  public void getUpgradeHiveTableProperties(Context ctx) throws IllegalAccessException {
    Map<String, String> keyValues = new TreeMap<>();
    Map<String, String> tableProperties =
        AmsUtil.getNotDeprecatedAndNotInternalStaticFields(TableProperties.class);
    tableProperties.keySet().stream()
        .filter(key -> !key.endsWith("_DEFAULT"))
        .forEach(
            key -> keyValues
                .put(tableProperties.get(key), tableProperties.get(key + "_DEFAULT")));
    ServerTableProperties.HIDDEN_EXPOSED.forEach(keyValues::remove);
    Map<String, String> hiveProperties =
        AmsUtil.getNotDeprecatedAndNotInternalStaticFields(HiveTableProperties.class);

    hiveProperties.keySet().stream()
        .filter(key -> HiveTableProperties.EXPOSED.contains(hiveProperties.get(key)))
        .filter(key -> !key.endsWith("_DEFAULT"))
        .forEach(
            key -> keyValues
                .put(hiveProperties.get(key), hiveProperties.get(key + "_DEFAULT")));
    ctx.json(OkResponse.of(keyValues));
  }

  /**
   * getRuntime optimize info.
   */
  public void getOptimizeInfo(Context ctx) {

    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);

    int offset = (page - 1) * pageSize;
    int limit = pageSize;
    Preconditions.checkArgument(offset >= 0, "offset[%s] must >= 0", offset);
    Preconditions.checkArgument(limit >= 0, "limit[%s] must >= 0", limit);
    Preconditions.checkState(tableService.tableExist(new com.netease.arctic.ams.api.TableIdentifier(catalog, db,
        table)), "no such table");

    List<OptimizedRecord> all = tableDescriptor.getOptimizeInfo(catalog, db, table);
    List<OptimizedRecord> result =
        all.stream().sorted(Comparator.comparingLong(OptimizedRecord::getCommitTime).reversed())
            .skip(offset)
            .limit(limit)
            .collect(Collectors.toList());
    ctx.json(OkResponse.of(PageResult.of(result, all.size())));
  }

  /**
   * getRuntime list of transactions.
   */
  public void getTableTransactions(Context ctx) {
    String catalogName = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String tableName = ctx.pathParam("table");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);

    List<TransactionsOfTable> transactionsOfTables =
        tableDescriptor.getTransactions(ServerTableIdentifier.of(catalogName, db, tableName));
    int offset = (page - 1) * pageSize;
    PageResult<TransactionsOfTable, AMSTransactionsOfTable> pageResult = PageResult.of(transactionsOfTables,
        offset, pageSize, AmsUtil::toTransactionsOfTable);
    ctx.json(OkResponse.of(pageResult));
  }

  /**
   * getRuntime detail of transaction.
   */
  public void getTransactionDetail(Context ctx) {
    String catalogName = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String tableName = ctx.pathParam("table");
    String transactionId = ctx.pathParam("transactionId");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);

    List<AMSDataFileInfo> result = tableDescriptor.getTransactionDetail(ServerTableIdentifier.of(catalogName, db,
        tableName), Long.parseLong(transactionId));
    int offset = (page - 1) * pageSize;
    PageResult<AMSDataFileInfo, AMSDataFileInfo> amsPageResult = PageResult.of(result,
        offset, pageSize);
    ctx.json(OkResponse.of(amsPageResult));
  }

  /**
   * getRuntime partition list.
   */
  public void getTablePartitions(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);

    List<PartitionBaseInfo> partitionBaseInfos = tableDescriptor.getTablePartition(
        ServerTableIdentifier.of(catalog, db, table));
    int offset = (page - 1) * pageSize;
    PageResult<PartitionBaseInfo, PartitionBaseInfo> amsPageResult = PageResult.of(partitionBaseInfos,
        offset, pageSize);
    ctx.json(OkResponse.of(amsPageResult));
  }

  /**
   * getRuntime file list of some partition.
   */
  public void getPartitionFileListInfo(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    String partition = ctx.pathParam("partition");

    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);

    List<PartitionFileBaseInfo> partitionFileBaseInfos = tableDescriptor.getTableFile(
        ServerTableIdentifier.of(catalog, db, table),
        partition,
        page * pageSize);
    int offset = (page - 1) * pageSize;
    PageResult<PartitionFileBaseInfo, PartitionFileBaseInfo> amsPageResult = PageResult.of(partitionFileBaseInfos,
        offset, pageSize);
    ctx.json(OkResponse.of(amsPageResult));
  }

  /* getRuntime  operations of some table*/
  public void getTableOperations(Context ctx) {
    String catalogName = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String tableName = ctx.pathParam("table");

    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);
    int offset = (page - 1) * pageSize;

    List<DDLInfo> ddlInfoList = tableDescriptor.getTableOperations(ServerTableIdentifier.of(catalogName, db,
        tableName));
    Collections.reverse(ddlInfoList);
    PageResult<DDLInfo, TableOperation> amsPageResult = PageResult.of(ddlInfoList,
        offset, pageSize, TableOperation::buildFromDDLInfo);
    ctx.json(OkResponse.of(amsPageResult));
  }

  /**
   * getRuntime table list of catalog.db.
   */
  public void getTableList(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String keywords = ctx.queryParam("keywords");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog) && StringUtils.isNotBlank(db),
        "catalog.database can not be empty in any element");

    List<ServerTableIdentifier> tableIdentifiers = tableService.listTables(catalog, db);
    ServerCatalog<?> serverCatalog = tableService.getServerCatalog(catalog);
    List<TableMeta> tables = new ArrayList<>();

    if (serverCatalog instanceof IcebergCatalogImpl) {
      tableIdentifiers.forEach(e -> tables.add(new TableMeta(
          e.getTableName(),
          TableMeta.TableType.ICEBERG.toString())));
    } else if (serverCatalog instanceof MixedHiveCatalogImpl) {
      tableIdentifiers.forEach(e -> tables.add(new TableMeta(e.getTableName(), TableMeta.TableType.ARCTIC.toString())));
      List<String> hiveTables = HiveTableUtil.getAllHiveTables(
          ((MixedHiveCatalogImpl) serverCatalog).getHiveClient(),
          db);
      Set<String> arcticTables =
          tableIdentifiers.stream().map(ServerTableIdentifier::getTableName).collect(Collectors.toSet());
      hiveTables.stream().filter(e -> !arcticTables.contains(e)).forEach(e -> tables.add(new TableMeta(
          e,
          TableMeta.TableType.HIVE.toString())));
    } else {
      tableIdentifiers.forEach(e -> tables.add(new TableMeta(e.getTableName(), TableMeta.TableType.ARCTIC.toString())));
    }
    ctx.json(OkResponse.of(tables.stream().filter(t -> StringUtils.isBlank(keywords) ||
        t.getName().contains(keywords)).collect(Collectors.toList())));
  }

  /**
   * getRuntime databases of some catalog.
   */
  public void getDatabaseList(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String keywords = ctx.queryParam("keywords");

    List<String> dbList = tableService.listDatabases(catalog).stream()
        .filter(item -> StringUtils.isBlank(keywords) || item.contains(keywords))
        .collect(Collectors.toList());
    ctx.json(OkResponse.of(dbList));
  }

  /**
   * list catalogs.
   */
  public void getCatalogs(Context ctx) {
    List<CatalogMeta> catalogs = tableService.listCatalogMetas();
    ctx.json(OkResponse.of(catalogs));
  }

  /**
   * getRuntime single page query token
   */
  public void getTableDetailTabToken(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");

    String signCal = CommonUtil.generateTablePageToken(catalog, db, table);
    ctx.json(OkResponse.of(signCal));
  }

  private List<AMSColumnInfo> transformHiveSchemaToAMSColumnInfo(List<FieldSchema> fields) {
    return fields.stream()
        .map(f -> {
          AMSColumnInfo columnInfo = new AMSColumnInfo();
          columnInfo.setField(f.getName());
          columnInfo.setType(f.getType());
          columnInfo.setComment(f.getComment());
          return columnInfo;
        }).collect(Collectors.toList());
  }
}
