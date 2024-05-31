package org.hypertrace.config.service.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
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
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.ConfigServiceUtils;
import org.hypertrace.core.documentstore.Document;

/**
 * This class represents the data model for the Document as stored by {@link DocumentConfigStore}.
 */
@lombok.Value
@Slf4j
public class ConfigDocument implements Document {

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  public static final String DEFAULT_LATEST_UPDATED_USER_ID = "Unknown";
  public static final String DEFAULT_LATEST_UPDATED_USER_EMAIL = "Unknown";
  public static final String RESOURCE_FIELD_NAME = "resourceName";
  public static final String RESOURCE_NAMESPACE_FIELD_NAME = "resourceNamespace";
  public static final String TENANT_ID_FIELD_NAME = "tenantId";
  public static final String CONTEXT_FIELD_NAME = "context";
  public static final String VERSION_FIELD_NAME = "configVersion";
  public static final String LAST_UPDATED_USER_ID_FIELD_NAME = "lastUpdateUserId";
  public static final String LAST_UPDATED_USER_EMAIL_FIELD_NAME = "lastUpdatedUserEmail";
  public static final String CONFIG_FIELD_NAME = "config";
  public static final String CREATION_TIMESTAMP_FIELD_NAME = "creationTimestamp";
  public static final String UPDATE_TIMESTAMP_FIELD_NAME = "updateTimestamp";

  @JsonProperty(value = RESOURCE_FIELD_NAME)
  String resourceName;

  @JsonProperty(value = RESOURCE_NAMESPACE_FIELD_NAME)
  String resourceNamespace;

  @JsonProperty(value = TENANT_ID_FIELD_NAME)
  String tenantId;

  @JsonProperty(value = CONTEXT_FIELD_NAME)
  String context;

  @JsonProperty(value = VERSION_FIELD_NAME)
  long configVersion;

  @JsonProperty(value = LAST_UPDATED_USER_ID_FIELD_NAME)
  String lastUpdatedUserId;

  @JsonProperty(value = LAST_UPDATED_USER_EMAIL_FIELD_NAME)
  String lastUpdatedUserEmail;

  @JsonSerialize(using = ValueSerializer.class)
  @JsonDeserialize(using = ValueDeserializer.class)
  @JsonProperty(value = CONFIG_FIELD_NAME)
  Value config;

  @JsonProperty(value = CREATION_TIMESTAMP_FIELD_NAME)
  long creationTimestamp;

  @JsonProperty(value = UPDATE_TIMESTAMP_FIELD_NAME)
  long updateTimestamp;

  @JsonCreator(mode = Mode.PROPERTIES)
  public ConfigDocument(
      @JsonProperty(RESOURCE_FIELD_NAME) String resourceName,
      @JsonProperty(RESOURCE_NAMESPACE_FIELD_NAME) String resourceNamespace,
      @JsonProperty(TENANT_ID_FIELD_NAME) String tenantId,
      @JsonProperty(CONTEXT_FIELD_NAME) String context,
      @JsonProperty(VERSION_FIELD_NAME) long configVersion,
      @JsonProperty(LAST_UPDATED_USER_ID_FIELD_NAME) String lastUpdatedUserId,
      @JsonProperty(LAST_UPDATED_USER_EMAIL_FIELD_NAME) String lastUpdatedUserEmail,
      @JsonProperty(CONFIG_FIELD_NAME) Value config,
      @JsonProperty(CREATION_TIMESTAMP_FIELD_NAME) long creationTimestamp,
      @JsonProperty(UPDATE_TIMESTAMP_FIELD_NAME) long updateTimestamp) {
    this.resourceName = resourceName;
    this.resourceNamespace = resourceNamespace;
    this.tenantId = tenantId;
    this.context = context;
    this.configVersion = configVersion;
    this.lastUpdatedUserId =
        Optional.ofNullable(lastUpdatedUserId).orElse(DEFAULT_LATEST_UPDATED_USER_ID);
    this.lastUpdatedUserEmail =
        Optional.ofNullable(lastUpdatedUserEmail).orElse(DEFAULT_LATEST_UPDATED_USER_EMAIL);
    this.config = config;
    this.creationTimestamp = creationTimestamp;
    this.updateTimestamp = updateTimestamp;
  }

  public static ConfigDocument fromJson(String json) throws IOException {
    return OBJECT_MAPPER.readValue(json, ConfigDocument.class);
  }

  @Override
  public String toJson() {
    try {
      return OBJECT_MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException ex) {
      log.error("Error in converting {} to json", this);
      throw new RuntimeException("Error in converting ConfigDocument to json", ex);
    }
  }

  public static class ValueSerializer extends JsonSerializer<Value> {

    @Override
    public void serialize(Value value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeRawValue(JsonFormat.printer().print(value));
    }
  }

  public static class ValueDeserializer extends JsonDeserializer<Value> {

    @Override
    public Value deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      String jsonString = p.readValueAsTree().toString();
      Value.Builder valueBuilder = Value.newBuilder();
      JsonFormat.parser().merge(jsonString, valueBuilder);
      return valueBuilder.build();
    }

    @Override
    public Value getNullValue(DeserializationContext ctxt) {
      return ConfigServiceUtils.emptyValue();
    }
  }
}
