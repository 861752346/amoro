package com.netease.arctic.server.dashboard;

import com.netease.arctic.ams.api.TableFormat;
import com.netease.arctic.data.DataFileType;
import com.netease.arctic.data.FileNameRules;
import com.netease.arctic.server.dashboard.model.AMSColumnInfo;
import com.netease.arctic.server.dashboard.model.AMSDataFileInfo;
import com.netease.arctic.server.dashboard.model.AMSPartitionField;
import com.netease.arctic.server.dashboard.model.DDLInfo;
import com.netease.arctic.server.dashboard.model.FilesStatistics;
import com.netease.arctic.server.dashboard.model.OptimizedRecord;
import com.netease.arctic.server.dashboard.model.PartitionBaseInfo;
import com.netease.arctic.server.dashboard.model.PartitionFileBaseInfo;
import com.netease.arctic.server.dashboard.model.ServerTableMeta;
import com.netease.arctic.server.dashboard.model.TableBasicInfo;
import com.netease.arctic.server.dashboard.model.TableOptimizingProcess;
import com.netease.arctic.server.dashboard.model.TableStatistics;
import com.netease.arctic.server.dashboard.model.TransactionsOfTable;
import com.netease.arctic.server.dashboard.response.OkResponse;
import com.netease.arctic.server.dashboard.utils.AmsUtil;
import com.netease.arctic.server.dashboard.utils.TableStatCollector;
import com.netease.arctic.server.optimizing.MetricsSummary;
import com.netease.arctic.server.persistence.PersistentBase;
import com.netease.arctic.server.persistence.mapper.OptimizingMapper;
import com.netease.arctic.server.persistence.mapper.TableMetaMapper;
import com.netease.arctic.server.table.ServerTableIdentifier;
import com.netease.arctic.server.table.TableService;
import com.netease.arctic.table.ATable;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.KeyedTable;
import com.netease.arctic.table.PrimaryKeySpec;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.table.UnkeyedTable;
import com.netease.arctic.trace.SnapshotSummary;
import com.netease.arctic.utils.ManifestEntryFields;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArcticTableDescriptor implements FormatTableDescriptor {

  private static final Logger LOG = LoggerFactory.getLogger(ArcticTableDescriptor.class);
  private static final Map<Integer, DataFileType> ICEBERG_FILE_TYPE_MAP = new HashMap<>();

  static {
    ICEBERG_FILE_TYPE_MAP.put(FileContent.DATA.id(), DataFileType.BASE_FILE);
    ICEBERG_FILE_TYPE_MAP.put(FileContent.POSITION_DELETES.id(), DataFileType.POS_DELETE_FILE);
    ICEBERG_FILE_TYPE_MAP.put(FileContent.EQUALITY_DELETES.id(), DataFileType.EQ_DELETE_FILE);
  }

  @Override
  public List<TableFormat> supportFormat() {
    return Arrays.asList(TableFormat.ICEBERG, TableFormat.MIXED_ICEBERG, TableFormat.MIXED_HIVE);
  }

  @Override
  public ServerTableMeta getTableDetail(ATable<?> aTable) {
    ArcticTable table = getArcticTable(aTable);
    // set basic info
    TableBasicInfo tableBasicInfo = getTableBasicInfo(table);
    ServerTableMeta serverTableMeta = getServerTableMeta(table);
    long tableSize = 0;
    long tableFileCnt = 0;
    Map<String, Object> baseMetrics = Maps.newHashMap();
    FilesStatistics baseFilesStatistics = tableBasicInfo.getBaseStatistics().getTotalFilesStat();
    Map<String, String> baseSummary = tableBasicInfo.getBaseStatistics().getSummary();
    baseMetrics.put("lastCommitTime", AmsUtil.longOrNull(baseSummary.get("visibleTime")));
    baseMetrics.put("totalSize", AmsUtil.byteToXB(baseFilesStatistics.getTotalSize()));
    baseMetrics.put("fileCount", baseFilesStatistics.getFileCnt());
    baseMetrics.put("averageFileSize", AmsUtil.byteToXB(baseFilesStatistics.getAverageSize()));
    baseMetrics.put("baseWatermark", AmsUtil.longOrNull(serverTableMeta.getBaseWatermark()));
    tableSize += baseFilesStatistics.getTotalSize();
    tableFileCnt += baseFilesStatistics.getFileCnt();
    serverTableMeta.setBaseMetrics(baseMetrics);

    Map<String, Object> changeMetrics = Maps.newHashMap();
    if (tableBasicInfo.getChangeStatistics() != null) {
      FilesStatistics changeFilesStatistics = tableBasicInfo.getChangeStatistics().getTotalFilesStat();
      Map<String, String> changeSummary = tableBasicInfo.getChangeStatistics().getSummary();
      changeMetrics.put("lastCommitTime", AmsUtil.longOrNull(changeSummary.get("visibleTime")));
      changeMetrics.put("totalSize", AmsUtil.byteToXB(changeFilesStatistics.getTotalSize()));
      changeMetrics.put("fileCount", changeFilesStatistics.getFileCnt());
      changeMetrics.put("averageFileSize", AmsUtil.byteToXB(changeFilesStatistics.getAverageSize()));
      changeMetrics.put("tableWatermark", AmsUtil.longOrNull(serverTableMeta.getTableWatermark()));
      tableSize += changeFilesStatistics.getTotalSize();
      tableFileCnt += changeFilesStatistics.getFileCnt();
    } else {
      changeMetrics.put("lastCommitTime", null);
      changeMetrics.put("totalSize", null);
      changeMetrics.put("fileCount", null);
      changeMetrics.put("averageFileSize", null);
      changeMetrics.put("tableWatermark", null);
    }
    serverTableMeta.setChangeMetrics(changeMetrics);

    Map<String, Object> tableSummary = new HashMap<>();
    tableSummary.put("size", AmsUtil.byteToXB(tableSize));
    tableSummary.put("file", tableFileCnt);
    tableSummary.put("averageFile", AmsUtil.byteToXB(tableFileCnt == 0 ? 0 : tableSize / tableFileCnt));
    tableSummary.put("tableFormat", AmsUtil.formatString(table.format().name()));
    serverTableMeta.setTableSummary(tableSummary);
    return serverTableMeta;
  }

  public List<TransactionsOfTable> getTransactions(ATable<?> aTable) {
    ArcticTable arcticTable = getArcticTable(aTable);
    List<TransactionsOfTable> transactionsOfTables = new ArrayList<>();
    List<Table> tables = new ArrayList<>();
    if (arcticTable.isKeyedTable()) {
      tables.add(arcticTable.asKeyedTable().changeTable());
      tables.add(arcticTable.asKeyedTable().baseTable());
    } else {
      tables.add(arcticTable.asUnkeyedTable());
    }
    tables.forEach(table -> table.snapshots().forEach(snapshot -> {
      if (snapshot.operation().equals(DataOperations.REPLACE)) {
        return;
      }
      if (snapshot.summary().containsKey(SnapshotSummary.TRANSACTION_BEGIN_SIGNATURE)) {
        return;
      }
      TransactionsOfTable transactionsOfTable = new TransactionsOfTable();
      transactionsOfTable.setTransactionId(snapshot.snapshotId());
      int fileCount = PropertyUtil
          .propertyAsInt(snapshot.summary(), org.apache.iceberg.SnapshotSummary.ADDED_FILES_PROP, 0);
      fileCount += PropertyUtil
          .propertyAsInt(snapshot.summary(), org.apache.iceberg.SnapshotSummary.ADDED_DELETE_FILES_PROP, 0);
      fileCount += PropertyUtil
          .propertyAsInt(snapshot.summary(), org.apache.iceberg.SnapshotSummary.DELETED_FILES_PROP, 0);
      fileCount += PropertyUtil
          .propertyAsInt(snapshot.summary(), org.apache.iceberg.SnapshotSummary.REMOVED_DELETE_FILES_PROP, 0);
      transactionsOfTable.setFileCount(fileCount);
      transactionsOfTable.setFileSize(PropertyUtil
          .propertyAsLong(snapshot.summary(), org.apache.iceberg.SnapshotSummary.ADDED_FILE_SIZE_PROP, 0) +
          PropertyUtil
              .propertyAsLong(snapshot.summary(), org.apache.iceberg.SnapshotSummary.REMOVED_FILE_SIZE_PROP, 0));
      transactionsOfTable.setCommitTime(snapshot.timestampMillis());
      transactionsOfTables.add(transactionsOfTable);
    }));
    transactionsOfTables.sort((o1, o2) -> Long.compare(o2.commitTime, o1.commitTime));
    return transactionsOfTables;
  }

  public List<AMSDataFileInfo> getTransactionDetail(ATable<?> aTable, long transactionId) {
    ArcticTable arcticTable = getArcticTable(aTable);
    List<AMSDataFileInfo> result = new ArrayList<>();
    Snapshot snapshot;
    if (arcticTable.isKeyedTable()) {
      snapshot = arcticTable.asKeyedTable().changeTable().snapshot(transactionId);
      if (snapshot == null) {
        snapshot = arcticTable.asKeyedTable().baseTable().snapshot(transactionId);
      }
    } else {
      snapshot = arcticTable.asUnkeyedTable().snapshot(transactionId);
    }
    if (snapshot == null) {
      throw new IllegalArgumentException("unknown snapshot " + transactionId + " of " + aTable.id());
    }
    final long snapshotTime = snapshot.timestampMillis();
    snapshot.addedDataFiles(arcticTable.io()).forEach(f -> {
      result.add(new AMSDataFileInfo(
          f.path().toString(),
          arcticTable.spec(), f.partition(),
          f.content(),
          f.fileSizeInBytes(),
          snapshotTime,
          "add"));
    });
    snapshot.removedDataFiles(arcticTable.io()).forEach(f -> {
      result.add(new AMSDataFileInfo(
          f.path().toString(),
          arcticTable.spec(), f.partition(),
          f.content(),
          f.fileSizeInBytes(),
          snapshotTime,
          "remove"));
    });
    snapshot.addedDeleteFiles(arcticTable.io()).forEach(f -> {
      result.add(new AMSDataFileInfo(
          f.path().toString(),
          arcticTable.spec(), f.partition(),
          f.content(),
          f.fileSizeInBytes(),
          snapshotTime,
          "add"));
    });
    snapshot.removedDeleteFiles(arcticTable.io()).forEach(f -> {
      result.add(new AMSDataFileInfo(
          f.path().toString(),
          arcticTable.spec(), f.partition(),
          f.content(),
          f.fileSizeInBytes(),
          snapshotTime,
          "remove"));
    });
    return result;
  }

  public List<DDLInfo> getTableOperations(ATable<?> aTable) {
    ArcticTable arcticTable = getArcticTable(aTable);
    List<DDLInfo> result = new ArrayList<>();
    Table table;
    if (arcticTable.isKeyedTable()) {
      table = arcticTable.asKeyedTable().baseTable();
    } else {
      table = arcticTable.asUnkeyedTable();
    }
    List<HistoryEntry> snapshotLog = ((HasTableOperations) table).operations().current().snapshotLog();
    List<org.apache.iceberg.TableMetadata.MetadataLogEntry> metadataLogEntries =
        ((HasTableOperations) table).operations().current().previousFiles();
    Set<Long> time = new HashSet<>();
    snapshotLog.forEach(e -> time.add(e.timestampMillis()));
    for (int i = 1; i < metadataLogEntries.size(); i++) {
      org.apache.iceberg.TableMetadata.MetadataLogEntry e = metadataLogEntries.get(i);
      if (!time.contains(e.timestampMillis())) {
        org.apache.iceberg.TableMetadata
            oldTableMetadata = TableMetadataParser.read(table.io(), metadataLogEntries.get(i - 1).file());
        org.apache.iceberg.TableMetadata
            newTableMetadata = TableMetadataParser.read(table.io(), metadataLogEntries.get(i).file());
        DDLInfo.Generator generator = new DDLInfo.Generator();
        result.addAll(generator.tableIdentify(arcticTable.id())
            .oldMeta(oldTableMetadata)
            .newMeta(newTableMetadata)
            .generate());
      }
    }
    if (metadataLogEntries.size() > 0) {
      org.apache.iceberg.TableMetadata oldTableMetadata = TableMetadataParser.read(
          table.io(),
          metadataLogEntries.get(metadataLogEntries.size() - 1).file());
      org.apache.iceberg.TableMetadata newTableMetadata = ((HasTableOperations) table).operations().current();
      DDLInfo.Generator generator = new DDLInfo.Generator();
      result.addAll(generator.tableIdentify(arcticTable.id())
          .oldMeta(oldTableMetadata)
          .newMeta(newTableMetadata)
          .generate());
    }
    return result;
  }

  public List<PartitionBaseInfo> getTablePartition(ATable<?> aTable) {
    ArcticTable arcticTable = getArcticTable(aTable);
    if (arcticTable.spec().isUnpartitioned()) {
      return new ArrayList<>();
    }
    Map<String, PartitionBaseInfo> partitionBaseInfoHashMap = new HashMap<>();
    getTableFile(aTable, null, Integer.MAX_VALUE).forEach(fileInfo -> {
      if (!partitionBaseInfoHashMap.containsKey(fileInfo.getPartitionName())) {
        partitionBaseInfoHashMap.put(fileInfo.getPartitionName(), new PartitionBaseInfo());
        partitionBaseInfoHashMap.get(fileInfo.getPartitionName()).setPartition(fileInfo.getPartitionName());
      }
      PartitionBaseInfo partitionInfo = partitionBaseInfoHashMap.get(fileInfo.getPartitionName());
      partitionInfo.setFileCount(partitionInfo.getFileCount() + 1);
      partitionInfo.setFileSize(partitionInfo.getFileSize() + fileInfo.getFileSize());
      partitionInfo.setLastCommitTime(partitionInfo.getLastCommitTime() > fileInfo.getCommitTime() ?
          partitionInfo.getLastCommitTime() :
          fileInfo.getCommitTime());
    });

    return new ArrayList<>(partitionBaseInfoHashMap.values());
  }

  public List<PartitionFileBaseInfo> getTableFile(ATable<?> aTable, String partition, int limit) {
    ArcticTable arcticTable = getArcticTable(aTable);
    List<PartitionFileBaseInfo> result = new ArrayList<>();
    if (arcticTable.isKeyedTable()) {
      result.addAll(collectFileInfo(arcticTable.asKeyedTable().changeTable(), true, partition, limit));
      result.addAll(collectFileInfo(arcticTable.asKeyedTable().baseTable(), false, partition, limit));
    } else {
      result.addAll(collectFileInfo(arcticTable.asUnkeyedTable(), false, partition, limit));
    }
    return result;
  }

  private ArcticTable getArcticTable(ATable<?> aTable) {
    return (ArcticTable) aTable.originalTable();
  }

  private List<PartitionFileBaseInfo> collectFileInfo(Table table, boolean isChangeTable, String partition, int limit) {
    PartitionSpec spec = table.spec();
    List<PartitionFileBaseInfo> result = new ArrayList<>();
    Table entriesTable = MetadataTableUtils.createMetadataTableInstance(((HasTableOperations) table).operations(),
        table.name(), table.name() + "#ENTRIES",
        MetadataTableType.ENTRIES);
    try (CloseableIterable<Record> manifests = IcebergGenerics.read(entriesTable)
        .where(Expressions.notEqual(ManifestEntryFields.STATUS.name(), ManifestEntryFields.Status.DELETED.id()))
        .build()) {
      for (Record record : manifests) {
        long snapshotId = (long) record.getField(ManifestEntryFields.SNAPSHOT_ID.name());
        GenericRecord dataFile = (GenericRecord) record.getField(ManifestEntryFields.DATA_FILE_FIELD_NAME);
        Integer contentId = (Integer) dataFile.getField(DataFile.CONTENT.name());
        String filePath = (String) dataFile.getField(DataFile.FILE_PATH.name());
        String partitionPath = null;
        GenericRecord parRecord = (GenericRecord) dataFile.getField(DataFile.PARTITION_NAME);
        if (parRecord != null) {
          InternalRecordWrapper wrapper = new InternalRecordWrapper(parRecord.struct());
          partitionPath = spec.partitionToPath(wrapper.wrap(parRecord));
        }
        if (partition != null && spec.isPartitioned() && !partition.equals(partitionPath)) {
          continue;
        }
        Long fileSize = (Long) dataFile.getField(DataFile.FILE_SIZE.name());
        DataFileType dataFileType =
            isChangeTable ? FileNameRules.parseFileTypeForChange(filePath) : ICEBERG_FILE_TYPE_MAP.get(contentId);
        long commitTime = -1;
        if (table.snapshot(snapshotId) != null) {
          commitTime = table.snapshot(snapshotId).timestampMillis();
        }
        result.add(new PartitionFileBaseInfo(snapshotId, dataFileType, commitTime,
            partitionPath, filePath, fileSize));
        if (result.size() >= limit) {
          return result;
        }
      }
    } catch (IOException exception) {
      LOG.error("close manifest file error", exception);
    }
    return result;
  }

  private TableBasicInfo getTableBasicInfo(ArcticTable table) {
    try {
      TableBasicInfo tableBasicInfo = new TableBasicInfo();
      tableBasicInfo.setTableIdentifier(table.id());
      TableStatistics changeInfo = null;
      TableStatistics baseInfo;

      if (table.isUnkeyedTable()) {
        UnkeyedTable unkeyedTable = table.asUnkeyedTable();
        baseInfo = new TableStatistics();
        TableStatCollector.fillTableStatistics(baseInfo, unkeyedTable, table);
      } else if (table.isKeyedTable()) {
        KeyedTable keyedTable = table.asKeyedTable();
        if (!PrimaryKeySpec.noPrimaryKey().equals(keyedTable.primaryKeySpec())) {
          changeInfo = TableStatCollector.collectChangeTableInfo(keyedTable);
        }
        baseInfo = TableStatCollector.collectBaseTableInfo(keyedTable);
      } else {
        throw new IllegalStateException("unknown type of table");
      }

      tableBasicInfo.setChangeStatistics(changeInfo);
      tableBasicInfo.setBaseStatistics(baseInfo);
      tableBasicInfo.setTableStatistics(TableStatCollector.union(changeInfo, baseInfo));

      long createTime
          = PropertyUtil.propertyAsLong(table.properties(), TableProperties.TABLE_CREATE_TIME,
          TableProperties.TABLE_CREATE_TIME_DEFAULT);
      if (createTime != TableProperties.TABLE_CREATE_TIME_DEFAULT) {
        if (tableBasicInfo.getTableStatistics() != null) {
          if (tableBasicInfo.getTableStatistics().getSummary() == null) {
            tableBasicInfo.getTableStatistics().setSummary(new HashMap<>());
          } else {
            LOG.warn("{} summary is null", table.id());
          }
          tableBasicInfo.getTableStatistics().getSummary()
              .put("createTime", String.valueOf(createTime));
        } else {
          LOG.warn("{} table statistics is null {}", table.id(), tableBasicInfo);
        }
      }
      return tableBasicInfo;
    } catch (Throwable t) {
      LOG.error("{} failed to build table basic info", table.id(), t);
      throw t;
    }
  }

  public ServerTableMeta getServerTableMeta(ArcticTable table) {
    ServerTableMeta serverTableMeta = new ServerTableMeta();
    serverTableMeta.setTableType(table.format().toString());
    serverTableMeta.setTableIdentifier(table.id());
    serverTableMeta.setBaseLocation(table.location());
    fillTableProperties(serverTableMeta, table.properties());
    serverTableMeta.setPartitionColumnList(table
        .spec()
        .fields()
        .stream()
        .map(item -> AMSPartitionField.buildFromPartitionSpec(table.spec().schema(), item))
        .collect(Collectors.toList()));
    serverTableMeta.setSchema(table
        .schema()
        .columns()
        .stream()
        .map(AMSColumnInfo::buildFromNestedField)
        .collect(Collectors.toList()));

    serverTableMeta.setFilter(null);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Table " + table.name() + " is keyedTable: {}", table instanceof KeyedTable);
    }
    if (table.isKeyedTable()) {
      KeyedTable kt = table.asKeyedTable();
      if (kt.primaryKeySpec() != null) {
        serverTableMeta.setPkList(kt
            .primaryKeySpec()
            .fields()
            .stream()
            .map(item -> AMSColumnInfo.buildFromPartitionSpec(table.spec().schema(), item))
            .collect(Collectors.toList()));
      }
    }
    if (serverTableMeta.getPkList() == null) {
      serverTableMeta.setPkList(new ArrayList<>());
    }
    return serverTableMeta;
  }

  private void fillTableProperties(
      ServerTableMeta serverTableMeta,
      Map<String, String> tableProperties) {
    Map<String, String> properties = com.google.common.collect.Maps.newHashMap(tableProperties);
    serverTableMeta.setTableWatermark(properties.remove(TableProperties.WATERMARK_TABLE));
    serverTableMeta.setBaseWatermark(properties.remove(TableProperties.WATERMARK_BASE_STORE));
    serverTableMeta.setCreateTime(PropertyUtil.propertyAsLong(properties, TableProperties.TABLE_CREATE_TIME,
        TableProperties.TABLE_CREATE_TIME_DEFAULT));
    properties.remove(TableProperties.TABLE_CREATE_TIME);

    TableProperties.READ_PROTECTED_PROPERTIES.forEach(properties::remove);
    serverTableMeta.setProperties(properties);
  }
}
