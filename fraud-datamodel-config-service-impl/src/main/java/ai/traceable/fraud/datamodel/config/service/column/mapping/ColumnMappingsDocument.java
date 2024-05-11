package ai.traceable.fraud.datamodel.config.service.column.mapping;

import ai.traceable.fraud.datamodel.config.service.v1.internal.InternalFieldMetadata;
import ai.traceable.fraud.datamodel.config.service.v1.internal.ObjectKind;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.Document;

/**
 * This class represents the data model for the Document as stored by {@link ColumnMappingsStore}.
 */
@lombok.Value
@Slf4j
public class ColumnMappingsDocument implements Document {
  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  public static final String TENANT_ID_FIELD_NAME = "tenantId";
  public static final String OBJECT_KIND_FIELD_NAME = "objectKind";
  public static final String OBJECT_TYPE_ID_FIELD_NAME = "objectTypeId";
  public static final String FIELD_NAME = "fieldName";
  public static final String COLUMN_ID = "columnId";
  public static final String FIELD_META = "internalFieldMeta";

  @JsonProperty(value = TENANT_ID_FIELD_NAME)
  String tenantId;

  @JsonProperty(value = OBJECT_KIND_FIELD_NAME)
  ObjectKind objectKind;

  @JsonProperty(value = OBJECT_TYPE_ID_FIELD_NAME)
  String objectTypeId;

  @JsonProperty(value = FIELD_NAME)
  String fieldName;

  @JsonProperty(value = COLUMN_ID)
  String columnId;

  @JsonSerialize(using = InternalFieldMetadataSerializer.class)
  @JsonDeserialize(using = InternalFieldMetadataDeSerializer.class)
  @JsonProperty(value = FIELD_META)
  InternalFieldMetadata internalFieldMetadata;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public ColumnMappingsDocument(
      @JsonProperty(TENANT_ID_FIELD_NAME) String tenantId,
      @JsonProperty(OBJECT_KIND_FIELD_NAME) ObjectKind objectKind,
      @JsonProperty(OBJECT_TYPE_ID_FIELD_NAME) String objectTypeId,
      @JsonProperty(FIELD_NAME) String fieldName,
      @JsonProperty(COLUMN_ID) String columnId,
      @JsonProperty(FIELD_META) InternalFieldMetadata internalFieldMetadata) {
    this.tenantId = tenantId;
    this.objectKind = objectKind;
    this.objectTypeId = objectTypeId;
    this.fieldName = fieldName;
    this.columnId = columnId;
    this.internalFieldMetadata = internalFieldMetadata;
  }

  public static ColumnMappingsDocument fromJson(String json) throws IOException {
    return OBJECT_MAPPER.readValue(json, ColumnMappingsDocument.class);
  }

  @Override
  public String toJson() {
    try {
      return OBJECT_MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException ex) {
      log.error("Error in converting {} to json", this);
      throw new RuntimeException("Error in converting ColumnMappingsDocument to json", ex);
    }
  }

  public static class InternalFieldMetadataSerializer
      extends JsonSerializer<InternalFieldMetadata> {

    @Override
    public void serialize(
        InternalFieldMetadata value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeRawValue(JsonFormat.printer().print(value));
    }
  }

  public static class InternalFieldMetadataDeSerializer
      extends JsonDeserializer<InternalFieldMetadata> {

    @Override
    public InternalFieldMetadata deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      String jsonString = p.readValueAsTree().toString();
      InternalFieldMetadata.Builder fieldMetadataBuilder = InternalFieldMetadata.newBuilder();
      JsonFormat.parser().merge(jsonString, fieldMetadataBuilder);
      return fieldMetadataBuilder.build();
    }

    @Override
    public InternalFieldMetadata getNullValue(DeserializationContext ctxt) {
      return InternalFieldMetadata.getDefaultInstance();
    }
  }
}
