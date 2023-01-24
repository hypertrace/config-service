package org.hypertrace.tenantpartitioning.config.service.store;

import com.google.protobuf.util.JsonFormat;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.tenantpartitioning.config.service.v1.TenantPartitionGroupsConfig;

public class TenantPartitionGroupsConfigDocument implements Document {

  private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  private final TenantPartitionGroupsConfig config;

  public TenantPartitionGroupsConfigDocument(
      TenantPartitionGroupsConfig tenantIsolationGroupConfig) {
    this.config = tenantIsolationGroupConfig;
  }

  public static TenantPartitionGroupsConfig fromJson(String json) {
    try {
      TenantPartitionGroupsConfig.Builder builder = TenantPartitionGroupsConfig.newBuilder();
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
