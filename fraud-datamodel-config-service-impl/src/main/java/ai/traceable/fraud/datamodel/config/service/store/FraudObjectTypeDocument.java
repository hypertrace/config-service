package ai.traceable.fraud.datamodel.config.service.store;

import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.Document;

/**
 * This class represents the data model for the Document as stored by {@link
 * FraudObjectTypesDocumentStore}.
 */
@lombok.Value
@Slf4j
public class FraudObjectTypeDocument implements Document {
  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  public static final String TENANT_ID_FIELD_NAME = "tenantId";
  public static final String OBJECT_KIND_FIELD_NAME = "objectKind";
  public static final String OBJECT_TYPE_ID_FIELD_NAME = "objectTypeId";
  public static final String CREATION_TIMESTAMP_FIELD_NAME = "creationTimestamp";
  public static final String UPDATE_TIMESTAMP_FIELD_NAME = "updateTimestamp";
  public static final String OBJECT_TYPE_FIELD_NAME = "objectType";

  @JsonProperty(value = TENANT_ID_FIELD_NAME)
  String tenantId;

  @JsonProperty(value = OBJECT_KIND_FIELD_NAME)
  ObjectKind objectKind;

  @JsonProperty(value = OBJECT_TYPE_ID_FIELD_NAME)
  String objectTypeId;

  @JsonSerialize(using = ObjectTypeSerializer.class)
  @JsonDeserialize(using = ObjectTypeDeserializer.class)
  @JsonProperty(value = OBJECT_TYPE_FIELD_NAME)
  ObjectType objectType;

  @JsonProperty(value = CREATION_TIMESTAMP_FIELD_NAME)
  long creationTimestamp;

  @JsonProperty(value = UPDATE_TIMESTAMP_FIELD_NAME)
  long updateTimestamp;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public FraudObjectTypeDocument(
      @JsonProperty(TENANT_ID_FIELD_NAME) String tenantId,
      @JsonProperty(OBJECT_KIND_FIELD_NAME) ObjectKind objectKind,
      @JsonProperty(OBJECT_TYPE_ID_FIELD_NAME) String objectTypeId,
      @JsonProperty(CREATION_TIMESTAMP_FIELD_NAME) long creationTimestamp,
      @JsonProperty(UPDATE_TIMESTAMP_FIELD_NAME) long updateTimestamp,
      @JsonProperty(OBJECT_TYPE_FIELD_NAME) ObjectType objectType) {
    this.tenantId = tenantId;
    this.objectKind = objectKind;
    this.objectTypeId = objectTypeId;
    this.objectType = objectType;
    this.creationTimestamp = creationTimestamp;
    this.updateTimestamp = updateTimestamp;
  }

  public static FraudObjectTypeDocument fromJson(String json) throws IOException {
    return OBJECT_MAPPER.readValue(json, FraudObjectTypeDocument.class);
  }

  @Override
  public String toJson() {
    try {
      return OBJECT_MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException ex) {
      log.error("Error in converting {} to json", this);
      throw new RuntimeException("Error in converting FraudObjectTypeDocument to json", ex);
    }
  }

  public static class ObjectTypeSerializer extends JsonSerializer<ObjectType> {

    @Override
    public void serialize(ObjectType value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeRawValue(JsonFormat.printer().print(value));
    }
  }

  public static class ObjectTypeDeserializer extends JsonDeserializer<ObjectType> {

    @Override
    public ObjectType deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      String jsonString = p.readValueAsTree().toString();
      ObjectType.Builder objectTypeBuilder = ObjectType.newBuilder();
      JsonFormat.parser().merge(jsonString, objectTypeBuilder);
      return objectTypeBuilder.build();
    }

    @Override
    public ObjectType getNullValue(DeserializationContext ctxt) {
      return ObjectType.getDefaultInstance();
    }
  }
}
