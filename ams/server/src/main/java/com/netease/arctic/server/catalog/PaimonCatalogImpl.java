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

package com.netease.arctic.server.catalog;

import com.netease.arctic.ams.api.CatalogMeta;
import com.netease.arctic.ams.api.TableIdentifier;
import com.netease.arctic.catalog.PaimonCatalogWrapper;
import com.netease.arctic.table.ATable;
import com.netease.arctic.table.PaimonTable;
import com.netease.arctic.utils.CatalogUtil;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.paimon.table.Table;

public class PaimonCatalogImpl extends ExternalCatalog<Table>{

  private PaimonCatalogWrapper catalogWrapper;

  protected PaimonCatalogImpl(CatalogMeta metadata) {
    super(metadata);
    this.catalogWrapper = new PaimonCatalogWrapper(getMetadata(), Collections.emptyMap());
  }

  @Override
  public boolean exist(String database) {
    return catalogWrapper.listDatabases().contains(database);
  }

  @Override
  public boolean exist(String database, String tableName) {
    return loadTable(database, tableName) != null;
  }

  @Override
  public List<String> listDatabases() {
    return catalogWrapper.listDatabases();
  }

  @Override
  public List<TableIdentifier> listTables() {
    return toAmsIdList(catalogWrapper.listTables());
  }

  @Override
  public List<TableIdentifier> listTables(String database) {
    return toAmsIdList(catalogWrapper.listTables(database));
  }

  @Override
  public ATable<Table> loadTable(String database, String tableName) {
    return new PaimonTable(
        catalogWrapper.loadTable(com.netease.arctic.table.TableIdentifier.of(name(), database, tableName)),
        com.netease.arctic.table.TableIdentifier.of(name(), database, tableName));
  }

  public List<TableIdentifier> toAmsIdList(List<com.netease.arctic.table.TableIdentifier> identifierList) {
    return identifierList.stream().map(CatalogUtil::amsTaleId).collect(Collectors.toList());
  }
}
