package org.hypertrace.partitioner.config.service.store;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.partitioner.config.service.v1.PartitionerProfile;

public class PartitionerProfileDocument implements Document {
  private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  private final PartitionerProfile partitionerProfile;

  public PartitionerProfileDocument(PartitionerProfile partitionerProfile) {
    this.partitionerProfile = partitionerProfile;
  }

  public static PartitionerProfile fromJson(String json) {
    PartitionerProfile.Builder builder = PartitionerProfile.newBuilder();
    try {
      JSON_PARSER.merge(json, builder);
      return builder.build();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toJson() {
    try {
      return JSON_PRINTER.print(partitionerProfile);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
