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

package com.netease.arctic.catalog;

import com.netease.arctic.AmsClient;
import com.netease.arctic.ams.api.CatalogMeta;
import com.netease.arctic.ams.api.properties.CatalogMetaProperties;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.table.TableMetaStore;
import com.netease.arctic.utils.CatalogUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;

/**
 * A wrapper class around {@link Catalog} and implement {@link ArcticCatalog}.
 */
public class PaimonCatalogWrapper {

  private CatalogMeta meta;
  private Map<String, String> customProperties;
  private Pattern databaseFilterPattern;
  private transient TableMetaStore tableMetaStore;
  private transient Catalog paimonCatalog;

  public String name() {
    return meta.getCatalogName();
  }

  public void initialize(AmsClient client, CatalogMeta meta, Map<String, String> properties) {

  }

  private void initialize(CatalogMeta meta, Map<String, String> properties) {
    this.meta = meta;
    this.customProperties = properties;
    CatalogUtil.mergeCatalogProperties(meta, properties);
    meta.putToCatalogProperties(
        "metastore",
        meta.getCatalogType());
    this.tableMetaStore = CatalogUtil.buildMetaStore(meta);

    Options options = new Options();
    options.set("warehouse", meta.catalogProperties.get("warehouse"));

    paimonCatalog =
        tableMetaStore.doAs(() -> CatalogFactory.createCatalog(CatalogContext.create(
            options,
            tableMetaStore.getConfiguration())));

    if (meta.getCatalogProperties().containsKey(CatalogMetaProperties.KEY_DATABASE_FILTER_REGULAR_EXPRESSION)) {
      String databaseFilter =
          meta.getCatalogProperties().get(CatalogMetaProperties.KEY_DATABASE_FILTER_REGULAR_EXPRESSION);
      databaseFilterPattern = Pattern.compile(databaseFilter);
    } else {
      databaseFilterPattern = null;
    }
  }

  public PaimonCatalogWrapper(CatalogMeta meta, Map<String, String> properties) {
    initialize(meta, properties);
  }

  public List<String> listDatabases() {
    List<String> databases =
        tableMetaStore.doAs(() ->
            paimonCatalog.listDatabases());
    return databases.stream()
        .filter(database -> databaseFilterPattern == null || databaseFilterPattern.matcher(database).matches())
        .collect(Collectors.toList());
  }

  public void createDatabase(String databaseName) {
    tableMetaStore.doAs(() -> {
      paimonCatalog.createDatabase(databaseName, false);
      return null;
    });
  }

  public void dropDatabase(String databaseName) {
    tableMetaStore.doAs(() -> {
      paimonCatalog.dropDatabase(databaseName, false, true);
      return null;
    });
  }

  public List<TableIdentifier> listTables(String database) {
    return tableMetaStore.doAs(() -> paimonCatalog.listTables(database).stream()
        .map(s -> TableIdentifier.of(name(), database, s))
        .collect(Collectors.toList()));
  }

  public List<TableIdentifier> listTables() {
    List<TableIdentifier> tables = new ArrayList<>();
    List<String> dbs = listDatabases();
    for (String db : dbs) {
      try {
        tables.addAll(listTables(db));
      } catch (Exception ignored) {
        continue;
      }
    }
    return tables;
  }

  public Table loadTable(TableIdentifier tableIdentifier) {
    Table table =
        tableMetaStore.doAs(() -> paimonCatalog.getTable(from(tableIdentifier)));
    return table;
  }

  public boolean tableExists(TableIdentifier tableIdentifier) {
    return paimonCatalog.tableExists(from(tableIdentifier));
  }

  public void renameTable(TableIdentifier from, String newTableName) {
    tableMetaStore.doAs(() -> {
      paimonCatalog.renameTable(from(from), new Identifier(from.getDatabase(), newTableName), true);
      return null;
    });
  }

  public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
    return tableMetaStore.doAs(
        () -> {
          paimonCatalog.dropTable(from(tableIdentifier), purge);
          return true;
        }
    );
  }

  public Map<String, String> properties() {
    return meta.getCatalogProperties();
  }

  public void refreshCatalogMeta(CatalogMeta meta) {
    initialize(meta, customProperties);
  }

  protected Identifier from(TableIdentifier tableIdentifier) {
    return new Identifier(tableIdentifier.getDatabase(), tableIdentifier.getTableName());
  }
}
