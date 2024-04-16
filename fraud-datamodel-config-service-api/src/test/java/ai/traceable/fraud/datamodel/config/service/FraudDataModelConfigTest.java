package ai.traceable.fraud.datamodel.config.service;

import ai.traceable.fraud.datamodel.config.service.v1.EntityType;
import ai.traceable.fraud.datamodel.config.service.v1.EventType;
import ai.traceable.fraud.datamodel.config.service.v1.MetricType;
import ai.traceable.fraud.datamodel.config.service.v1.RelationshipType;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FraudDataModelConfigTest {
  private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  public static String serialize(Message m) {
    try {
      return JSON_PRINTER.print(m);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T extends Message.Builder> T deserialize(String value, T builder) {
    try {
      JSON_PARSER.merge(value, builder);
      return builder;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testSerDe() {
    var entityTypes = FraudDataModelTestUtils.entityTypes();
    for (var entityType : entityTypes) {
      var serialized = serialize(entityType);
      var deserialized = deserialize(serialized, EntityType.newBuilder()).build();
      Assertions.assertEquals(entityType, deserialized);
    }

    var relTypes = FraudDataModelTestUtils.relationshipTypes();
    for (var relType : relTypes) {
      var serialized = serialize(relType);
      var deserialized = deserialize(serialized, RelationshipType.newBuilder()).build();
      Assertions.assertEquals(relType, deserialized);
    }

    var eventTypes = FraudDataModelTestUtils.eventTypes();
    for (var eventType : eventTypes) {
      var serialized = serialize(eventType);
      var deserialized = deserialize(serialized, EventType.newBuilder()).build();
      Assertions.assertEquals(eventType, deserialized);
    }

    var metricTypes = FraudDataModelTestUtils.metricTypes();
    for (var metricType : metricTypes) {
      var serialized = serialize(metricType);
      var deserialized = deserialize(serialized, MetricType.newBuilder()).build();
      Assertions.assertEquals(metricType, deserialized);
    }
  }
}
