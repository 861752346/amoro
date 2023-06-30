package com.netease.arctic.server.dashboard;

import com.netease.arctic.ams.api.TableFormat;
import com.netease.arctic.data.DataFileType;
import com.netease.arctic.data.FileNameRules;
import com.netease.arctic.server.dashboard.model.AMSDataFileInfo;
import com.netease.arctic.server.dashboard.model.DDLInfo;
import com.netease.arctic.server.dashboard.model.FilesStatistics;
import com.netease.arctic.server.dashboard.model.OptimizedRecord;
import com.netease.arctic.server.dashboard.model.PartitionBaseInfo;
import com.netease.arctic.server.dashboard.model.PartitionFileBaseInfo;
import com.netease.arctic.server.dashboard.model.ServerTableMeta;
import com.netease.arctic.server.dashboard.model.TableOptimizingProcess;
import com.netease.arctic.server.dashboard.model.TransactionsOfTable;
import com.netease.arctic.server.optimizing.MetricsSummary;
import com.netease.arctic.server.persistence.PersistentBase;
import com.netease.arctic.server.persistence.mapper.OptimizingMapper;
import com.netease.arctic.server.persistence.mapper.TableMetaMapper;
import com.netease.arctic.server.table.ServerTableIdentifier;
import com.netease.arctic.server.table.TableService;
import com.netease.arctic.table.ATable;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.trace.SnapshotSummary;
import com.netease.arctic.utils.ManifestEntryFields;
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
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ServerTableDescriptor extends PersistentBase {

  private static final Logger LOG = LoggerFactory.getLogger(ServerTableDescriptor.class);

  private final Map<TableFormat, FormatTableDescriptor> formatDescriptorMap = new HashMap<>();

  private final TableService tableService;

  {
    FormatTableDescriptor[] formatTableDescriptors = new FormatTableDescriptor[]{
      new ArcticTableDescriptor(),
      new PaimonTableDescriptor()
    };
    for (FormatTableDescriptor formatTableDescriptor: formatTableDescriptors) {
      for (TableFormat format: formatTableDescriptor.supportFormat()) {
        formatDescriptorMap.put(format, formatTableDescriptor);
      }
    }
  }

  public ServerTableDescriptor(TableService tableService) {
    this.tableService = tableService;
  }

  private ServerTableIdentifier getTable(String catalog, String db, String table) {
    return getAs(TableMetaMapper.class, mapper -> mapper.selectTableIdentifier(catalog, db, table));
  }

  public ServerTableMeta getTableDetail(ServerTableIdentifier tableIdentifier) {
    ATable<?> aTable = tableService.loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(aTable.format());
    return formatTableDescriptor.getTableDetail(aTable);
  }

  public List<TransactionsOfTable> getTransactions(ServerTableIdentifier tableIdentifier) {
    ATable<?> aTable = tableService.loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(aTable.format());
    return formatTableDescriptor.getTransactions(aTable);
  }

  public List<AMSDataFileInfo> getTransactionDetail(ServerTableIdentifier tableIdentifier, long transactionId) {
    ATable<?> aTable = tableService.loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(aTable.format());
    return formatTableDescriptor.getTransactionDetail(aTable, transactionId);
  }

  public List<DDLInfo> getTableOperations(ServerTableIdentifier tableIdentifier) {
    ATable<?> aTable = tableService.loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(aTable.format());
    return formatTableDescriptor.getTableOperations(aTable);
  }

  public List<OptimizedRecord> getOptimizeInfo(String catalog, String db, String table) {
    List<TableOptimizingProcess> tableOptimizingProcesses = getAs(
        OptimizingMapper.class,
        mapper -> mapper.selectSuccessOptimizingProcesses(catalog, db, table));
    return tableOptimizingProcesses.stream().map(optimizingProcess -> {
      OptimizedRecord record = new OptimizedRecord();
      record.setCommitTime(optimizingProcess.getEndTime());
      record.setPlanTime(optimizingProcess.getPlanTime());
      record.setDuration(optimizingProcess.getEndTime() - optimizingProcess.getPlanTime());
      record.setTableIdentifier(TableIdentifier.of(optimizingProcess.getCatalogName(), optimizingProcess.getDbName(),
          optimizingProcess.getTableName()));
      record.setOptimizeType(optimizingProcess.getOptimizingType());
      MetricsSummary metricsSummary = optimizingProcess.getSummary();
      record.setTotalFilesStatBeforeCompact(FilesStatistics.builder()
          .addFiles(metricsSummary.getEqualityDeleteSize(), metricsSummary.getEqDeleteFileCnt())
          .addFiles(metricsSummary.getPositionalDeleteSize(), metricsSummary.getPosDeleteFileCnt())
          .addFiles(metricsSummary.getRewriteDataSize(), metricsSummary.getRewriteDataFileCnt())
          .build());
      record.setTotalFilesStatAfterCompact(FilesStatistics.build(
          metricsSummary.getNewFileCnt(),
          metricsSummary.getNewFileSize()));
      return record;
    }).collect(Collectors.toList());
  }

  public List<PartitionBaseInfo> getTablePartition(ServerTableIdentifier tableIdentifier) {
    ATable<?> aTable = tableService.loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(aTable.format());
    return formatTableDescriptor.getTablePartition(aTable);
  }

  public List<PartitionFileBaseInfo> getTableFile(ServerTableIdentifier tableIdentifier, String partition, int limit) {
    ATable<?> aTable = tableService.loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(aTable.format());
    return formatTableDescriptor.getTableFile(aTable, partition, limit);
  }
}
