package ai.traceable.fraud.datamodel.config.service;

import ai.traceable.fraud.datamodel.config.service.v1.Cardinality;
import ai.traceable.fraud.datamodel.config.service.v1.EntityFieldMetadata;
import ai.traceable.fraud.datamodel.config.service.v1.EntityType;
import ai.traceable.fraud.datamodel.config.service.v1.EventType;
import ai.traceable.fraud.datamodel.config.service.v1.FieldMetadata;
import ai.traceable.fraud.datamodel.config.service.v1.FieldType;
import ai.traceable.fraud.datamodel.config.service.v1.IdFieldSet;
import ai.traceable.fraud.datamodel.config.service.v1.MetricDataType;
import ai.traceable.fraud.datamodel.config.service.v1.MetricType;
import ai.traceable.fraud.datamodel.config.service.v1.RelationshipType;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FraudDataModelTestUtils {
  public static final List<EntityType> entityTypes = entityTypes();
  public static final List<RelationshipType> relationshipTypes = relationshipTypes();
  public static final List<EventType> eventTypes = eventTypes();
  public static final List<MetricType> metricTypes = metricTypes();
  public static final Set<String> entityTypeIds = new HashSet<>();
  public static final Set<String> relationshipTypeIds = new HashSet<>();
  public static final Set<String> eventTypeIds = new HashSet<>();
  public static final Set<String> metricTypeIds = new HashSet<>();
  public static final Map<String, EntityType> entityTypeMap = new HashMap<>();
  public static final Map<String, RelationshipType> relationshipTypeMap = new HashMap<>();
  public static final Map<String, EventType> eventTypeMap = new HashMap<>();
  public static final Map<String, MetricType> metricTypeMap = new HashMap<>();

  static {
    entityTypes.forEach(
        type -> {
          String id = type.getId();
          entityTypeMap.put(id, type);
          entityTypeIds.add(id);
        });
    relationshipTypes.forEach(
        type -> {
          String id = type.getId();
          relationshipTypeMap.put(id, type);
          relationshipTypeIds.add(id);
        });
    metricTypes.forEach(
        type -> {
          String id = type.getId();
          metricTypeMap.put(id, type);
          metricTypeIds.add(id);
        });
    eventTypes.forEach(
        type -> {
          String id = type.getId();
          eventTypeMap.put(id, type);
          eventTypeIds.add(id);
        });
  }

  public static List<EntityType> getEntityTypes() {
    return entityTypes;
  }

  public static List<RelationshipType> getRelationshipTypes() {
    return relationshipTypes;
  }

  public static List<EventType> getEventTypes() {
    return eventTypes;
  }

  public static List<MetricType> getMetricTypes() {
    return metricTypes;
  }

  public static List<EntityType> entityTypes() {
    return List.of(
        entityType("User"),
        entityType("Account"),
        entityType("Bank"),
        entityType("PaymentType"),
        entityType("Phone"),
        entityType("Email"),
        entityType("Device"),
        entityType("IP"),
        entityType("ZipCode"),
        entityType("IP_ORG"),
        entityType("IP_ASN"),
        entityType("City"),
        entityType("State"),
        entityType("Country"),
        entityType("API"));
  }

  public static List<RelationshipType> relationshipTypes() {
    return List.of(
        relationshipType("user_owns_account", "User", "Account"),
        relationshipType("user_transfers_to_account", "User", "Account"),
        relationshipType("user_received_from_account", "User", "Account"),
        relationshipType("account_belongs_to_bank", "Account", "Bank"),
        relationshipType("device_accessed_account", "Device", "Account"),
        relationshipType("ip_accessed_account", "IP", "Bank"),
        relationshipType("phone_accessed_account", "Phone", "Bank"),
        relationshipType("email_accessed_account", "Email", "Bank"),
        relationshipType("user_has_email", "User", "Email"),
        relationshipType("user_has_phone", "User", "Phone"),
        relationshipType("user_has_device", "User", "Device"),
        relationshipType("ip_belongs_to_org", "IP", "IP_ORG"),
        relationshipType("ip_belongs_to_asn", "IP", "IP_ASN"),
        relationshipType("zip_belongs_to_city", "ZipCode", "City"),
        relationshipType("city_belongs_to_state", "City", "State"),
        relationshipType("state_belongs_to_country", "State", "Country"),
        relationshipType("user_accessed_api", "User", "API"),
        relationshipType("api_accessed_account", "API", "Account"));
  }

  public static List<MetricType> metricTypes() {
    return List.of(
        metricType("metric1"), metricType("metric2"), metricType("metric3"), metricType("metric4"));
  }

  public static List<EventType> eventTypes() {
    return List.of(customerEventsEventType());
  }

  public static EntityType entityType(String id) {
    return entityType(id, Collections.emptyMap());
  }

  public static EntityType entityType(String id, int numFields) {
    IdFieldSet idFieldSet = IdFieldSet.newBuilder().addField("id").build();
    EntityType.Builder entityTypeBuilder =
        EntityType.newBuilder()
            .setId(id)
            .setIdSet(idFieldSet)
            .putFieldsMeta(
                "id",
                EntityFieldMetadata.newBuilder().setFieldType(FieldType.FIELD_TYPE_STR).build());
    for (int ii = 0; ii < numFields; ii++) {
      for (var fieldType : FieldType.values()) {
        if (fieldType == FieldType.UNRECOGNIZED || fieldType == FieldType.FIELD_TYPE_UNSPECIFIED) {
          continue;
        }
        var fieldName = String.format("Field_%s_%s", fieldType, ii);
        entityTypeBuilder.putFieldsMeta(
            fieldName, EntityFieldMetadata.newBuilder().setFieldType(fieldType).build());
      }
    }
    return entityTypeBuilder.build();
  }

  public static MetricType metricType(String id, int numFields) {
    MetricType.Builder metricTypeBuilder =
        MetricType.newBuilder()
            .setId(id)
            .putFieldsMeta(
                "id", FieldMetadata.newBuilder().setFieldType(FieldType.FIELD_TYPE_STR).build());
    for (int ii = 0; ii < numFields; ii++) {
      for (var dataType : FieldType.values()) {
        if (dataType == FieldType.UNRECOGNIZED || dataType == FieldType.FIELD_TYPE_UNSPECIFIED) {
          continue;
        }
        var fieldName = String.format("Field_%s_%s", dataType, ii);
        metricTypeBuilder.putFieldsMeta(
            fieldName, FieldMetadata.newBuilder().setFieldType(dataType).build());
      }
    }
    return metricTypeBuilder.build();
  }

  public static EventType eventType(String id, int numFields) {
    EventType.Builder eventTypeBuilder =
        EventType.newBuilder()
            .setId(id)
            .putFieldsMeta(
                "id", FieldMetadata.newBuilder().setFieldType(FieldType.FIELD_TYPE_STR).build());
    for (int ii = 0; ii < numFields; ii++) {
      for (var dataType : FieldType.values()) {
        if (dataType == FieldType.UNRECOGNIZED || dataType == FieldType.FIELD_TYPE_UNSPECIFIED) {
          continue;
        }
        var fieldName = String.format("Field_%s_%s", dataType, ii);
        eventTypeBuilder.putFieldsMeta(
            fieldName, FieldMetadata.newBuilder().setFieldType(dataType).build());
      }
    }
    return eventTypeBuilder.build();
  }

  public static EntityType entityType(String id, Map<String, EntityFieldMetadata> fields) {
    IdFieldSet idFieldSet = IdFieldSet.newBuilder().addField("id").build();
    EntityType.Builder entityTypeBuilder =
        EntityType.newBuilder()
            .setId(id)
            .setIdSet(idFieldSet)
            .putFieldsMeta(
                "id",
                EntityFieldMetadata.newBuilder().setFieldType(FieldType.FIELD_TYPE_STR).build())
            .putAllFieldsMeta(fields);
    return entityTypeBuilder.build();
  }

  public static RelationshipType relationshipType(String left, String right) {
    return relationshipType(left + "_" + right, left, right);
  }

  public static RelationshipType relationshipType(
      String id, String leftTypeId, String rightTypeId) {
    RelationshipType.Builder relationshipTypeBuilder =
        RelationshipType.newBuilder()
            .setId(id)
            .setLeftEntityTypeId(leftTypeId)
            .setRightEntityTypeId(rightTypeId)
            .setLeftToRightNavName(leftTypeId)
            .setRightToLeftNavName(rightTypeId)
            .setLeftCardinality(Cardinality.CARDINALITY_MANY)
            .setRightCardinality(Cardinality.CARDINALITY_MANY);
    return relationshipTypeBuilder.build();
  }

  public static MetricType metricType(String id) {
    MetricType.Builder metricTypeBuilder =
        MetricType.newBuilder()
            .setId(id)
            .setTimestampField("ts")
            .putFieldsMeta(
                "ts", FieldMetadata.newBuilder().setFieldType(FieldType.FIELD_TYPE_LONG).build())
            .setMetricDataType(MetricDataType.METRIC_DATA_TYPE_COUNTER);
    return metricTypeBuilder.build();
  }

  public static EventType eventType(String id) {
    EventType.Builder eventTypeBuilder =
        EventType.newBuilder()
            .setId(id)
            .setTimestampField("ts")
            .putFieldsMeta(
                "ts", FieldMetadata.newBuilder().setFieldType(FieldType.FIELD_TYPE_LONG).build());
    return eventTypeBuilder.build();
  }

  public static EventType spanEventViewEventType() {
    EventType.Builder eventTypeBuilder = EventType.newBuilder().setId("span_event_view");
    // declare the columns here.
    return eventTypeBuilder.build();
  }

  public static EventType customerEventsEventType() {
    EventType.Builder eventTypeBuilder = EventType.newBuilder().setId("customer_events");
    // each entity type is a column, to record the entity id participating in an event
    // each relationship type is a bool column, to record whether a relationship type is
    // participating in an event
    //  this can also be modeled as an array of relationship types.
    for (var entityType : entityTypes()) {
      var entityTypeId = entityType.getId();
      eventTypeBuilder.putFieldsMeta(
          entityTypeId,
          FieldMetadata.newBuilder()
              .setFieldType(FieldType.FIELD_TYPE_STR)
              .setEntityType(entityTypeId)
              .build());
    }
    for (var relType : relationshipTypes()) {
      var relTypeId = relType.getId();
      eventTypeBuilder.putFieldsMeta(
          relTypeId,
          FieldMetadata.newBuilder()
              .setFieldType(FieldType.FIELD_TYPE_BOOL)
              .setRelationshipType(relTypeId)
              .build());
    }
    return eventTypeBuilder.build();
  }
}
