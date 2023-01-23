package org.hypertrace.tenantpartitioning.config.service.store;

import com.google.protobuf.util.JsonFormat;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.tenantpartitioning.config.service.v1.TenantPartitionGroupConfig;

public class TenantPartitionGroupConfigDocument implements Document {

  private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  private final TenantPartitionGroupConfig config;

  public TenantPartitionGroupConfigDocument(TenantPartitionGroupConfig tenantIsolationGroupConfig) {
    this.config = tenantIsolationGroupConfig;
  }

  public static TenantPartitionGroupConfig fromJson(String json) {
    try {
      TenantPartitionGroupConfig.Builder builder = TenantPartitionGroupConfig.newBuilder();
      JSON_PARSER.merge(json, builder);
      return builder.build();
    } catch (Exception e) {
      throw new RuntimeException(
          "Error in converting json string to TenantIsolationGroupConfig object", e);
    }
  }

  @Override
  public String toJson() {
    try {
      return JSON_PRINTER.print(config);
    } catch (Exception e) {
      throw new RuntimeException(
          "Error in converting TenantIsolationGroupConfig document to json", e);
    }
  }
}
