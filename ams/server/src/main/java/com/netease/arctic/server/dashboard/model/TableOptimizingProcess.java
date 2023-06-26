package com.netease.arctic.server.dashboard.model;

import com.netease.arctic.server.optimizing.MetricsSummary;
import com.netease.arctic.server.optimizing.OptimizingType;
import com.netease.arctic.table.ATable;

public class TableOptimizingProcess {

  private Long processId;
  private Long tableId;
  private String catalogName;
  private String dbName;
  private String tableName;
  private ATable.Snapshot fromSnapshot;
  private String status;
  private OptimizingType optimizingType;
  private long planTime;
  private long endTime;
  private String failReason;
  private MetricsSummary summary;

  public TableOptimizingProcess() {
  }

  public Long getProcessId() {
    return processId;
  }

  public void setProcessId(Long processId) {
    this.processId = processId;
  }

  public Long getTableId() {
    return tableId;
  }

  public void setTableId(Long tableId) {
    this.tableId = tableId;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public void setCatalogName(String catalogName) {
    this.catalogName = catalogName;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public ATable.Snapshot getFromSnapshot() {
    return fromSnapshot;
  }

  public void setFromSnapshot(ATable.Snapshot fromSnapshot) {
    this.fromSnapshot = fromSnapshot;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public OptimizingType getOptimizingType() {
    return optimizingType;
  }

  public void setOptimizingType(OptimizingType optimizingType) {
    this.optimizingType = optimizingType;
  }

  public long getPlanTime() {
    return planTime;
  }

  public void setPlanTime(long planTime) {
    this.planTime = planTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public String getFailReason() {
    return failReason;
  }

  public void setFailReason(String failReason) {
    this.failReason = failReason;
  }

  public MetricsSummary getSummary() {
    return summary;
  }

  public void setSummary(MetricsSummary summary) {
    this.summary = summary;
  }
}
