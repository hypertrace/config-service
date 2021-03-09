package org.hypertrace.config.service.store;

import static org.hypertrace.config.service.ConfigServiceUtils.DEFAULT_CONTEXT;
import static org.hypertrace.config.service.TestUtils.RESOURCE_NAME;
import static org.hypertrace.config.service.TestUtils.RESOURCE_NAMESPACE;
import static org.hypertrace.config.service.TestUtils.TENANT_ID;
import static org.hypertrace.config.service.TestUtils.getConfig1;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.io.Resources;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import java.io.IOException;
import java.nio.charset.Charset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class ConfigDocumentTest {

  @Test
  void convertToAndFromJson() throws IOException {
    long timestamp = System.currentTimeMillis();
    ConfigDocument configDocument =
        new ConfigDocument(
            RESOURCE_NAME,
            RESOURCE_NAMESPACE,
            TENANT_ID,
            DEFAULT_CONTEXT,
            15,
            "user1",
            getConfig1(),
            timestamp,
            timestamp);
    assertEquals(configDocument, ConfigDocument.fromJson(configDocument.toJson()));
  }

  @Test
  void convertDocumentContainingNullValue() throws IOException {
    long timestamp = System.currentTimeMillis();
    Value nullValue = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
    ConfigDocument configDocument =
        new ConfigDocument(
            RESOURCE_NAME,
            RESOURCE_NAMESPACE,
            TENANT_ID,
            DEFAULT_CONTEXT,
            15,
            "user1",
            nullValue,
            timestamp,
            timestamp);
    assertEquals(configDocument, ConfigDocument.fromJson(configDocument.toJson()));
  }

  @Test
  @DisplayName(
      "Test backward compatibility using a json string without creation and update timestamps")
  void convertJsonStringWithoutTimestamp() throws IOException {
    String jsonString =
        Resources.toString(Resources.getResource("config1.json"), Charset.defaultCharset());
    ConfigDocument configDocument = ConfigDocument.fromJson(jsonString);

    assertEquals("config1", configDocument.getResourceName());
    assertEquals("namespace1", configDocument.getResourceNamespace());
    assertEquals("tenant1", configDocument.getTenantId());
    assertEquals("DEFAULT-CONTEXT", configDocument.getContext());
    assertEquals(1, configDocument.getConfigVersion());
    assertEquals(getConfig1(), configDocument.getConfig());
    assertEquals(0, configDocument.getCreationTimestamp());
    assertEquals(0, configDocument.getUpdateTimestamp());
  }
}
