package com.netease.arctic.server.optimizing;

import com.netease.arctic.table.ATable;
import java.util.Map;

public interface OptimizingProcess {

  long getProcessId();

  void close();

  boolean isClosed();

  ATable.Snapshot getFromSnapshot();

  long getPlanTime();

  long getDuration();

  OptimizingType getOptimizingType();

  Status getStatus();

  long getRunningQuotaTime(long calculatingStartTime, long calculatingEndTime);

  void commit();

  MetricsSummary getSummary();

  enum Status {
    RUNNING,
    CLOSED,
    SUCCESS,
    FAILED;
  }
}
