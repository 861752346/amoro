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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;

import static org.apache.paimon.options.CatalogOptions.METASTORE;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.utils.Preconditions.checkArgument;

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

    CatalogContext catalogContext = CatalogContext.create(options, tableMetaStore.getConfiguration());

    paimonCatalog =
        tableMetaStore.doAs(() -> createCatalog(catalogContext));

    if (meta.getCatalogProperties().containsKey(CatalogMetaProperties.KEY_DATABASE_FILTER_REGULAR_EXPRESSION)) {
      String databaseFilter =
          meta.getCatalogProperties().get(CatalogMetaProperties.KEY_DATABASE_FILTER_REGULAR_EXPRESSION);
      databaseFilterPattern = Pattern.compile(databaseFilter);
    } else {
      databaseFilterPattern = null;
    }
  }

  // To replace FileIo with kerberos-supported FileIo
  private Catalog createCatalog(CatalogContext context) {
    String warehouse = CatalogFactory.warehouse(context).toUri().toString();
    String metastore = context.options().get(METASTORE);
    List<CatalogFactory> factories = new ArrayList<>();
    ServiceLoader.load(CatalogFactory.class)
        .iterator()
        .forEachRemaining(
            f -> {
              if (f.identifier().equals(metastore)) {
                factories.add(f);
              }
            });
    if (factories.size() != 1) {
      throw new RuntimeException(
          "Found "
              + factories.size()
              + " classes implementing "
              + CatalogFactory.class.getName()
              + " with metastore "
              + metastore
              + ". They are:\n"
              + factories.stream()
              .map(t -> t.getClass().getName())
              .collect(Collectors.joining("\n")));
    }

    Path warehousePath = new Path(warehouse);
    FileIO fileIO;

    try {
      fileIO = new SupportKerberosFileIO(FileIO.get(warehousePath, context), tableMetaStore);
      if (fileIO.exists(warehousePath)) {
        checkArgument(
            fileIO.isDir(warehousePath),
            "The %s path '%s' should be a directory.",
            WAREHOUSE.key(),
            warehouse);
      } else {
        fileIO.mkdirs(warehousePath);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return factories.get(0).create(fileIO, warehousePath, context);
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

  private static class SupportKerberosFileIO implements FileIO {

    private FileIO fileIO;

    private TableMetaStore tableMetaStore;

    public SupportKerberosFileIO(FileIO fileIO, TableMetaStore tableMetaStore) {
      this.fileIO = fileIO;
      this.tableMetaStore = tableMetaStore;
    }

    @Override
    public boolean isObjectStore() {
      return fileIO.isObjectStore();
    }

    @Override
    public void configure(CatalogContext catalogContext) {
      fileIO.configure(catalogContext);
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
      return tableMetaStore.doAs(
          () -> fileIO.newInputStream(path)
      );
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean b) throws IOException {
      return tableMetaStore.doAs(
          () -> fileIO.newOutputStream(path, b)
      );
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      return tableMetaStore.doAs(
          () -> fileIO.getFileStatus(path)
      );
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
      return tableMetaStore.doAs(
          () -> fileIO.listStatus(path)
      );
    }

    @Override
    public boolean exists(Path path) throws IOException {
      return tableMetaStore.doAs(
          () -> fileIO.exists(path)
      );
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
      return tableMetaStore.doAs(
          () -> fileIO.delete(path, b)
      );
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
      return tableMetaStore.doAs(
          () -> fileIO.mkdirs(path)
      );
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
      return tableMetaStore.doAs(
          () -> fileIO.rename(path, path1)
      );
    }
  }
}
