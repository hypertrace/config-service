package org.hypertrace.tenantisolation.config.service.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hypertrace.core.documentstore.Document;

public class TenantIsolationGroupConfigDocument implements Document {

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private final TenantIsolationGroupConfigDTO tenantIsolationGroupConfigDTO;

  public TenantIsolationGroupConfigDocument(
      TenantIsolationGroupConfigDTO tenantIsolationGroupConfigDTO) {
    this.tenantIsolationGroupConfigDTO = tenantIsolationGroupConfigDTO;
  }

  public static TenantIsolationGroupConfigDTO fromJson(String json) {
    try {
      return OBJECT_MAPPER.readValue(json, TenantIsolationGroupConfigDTO.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          "Error in converting json string to TenantIsolationGroupConfigDTO object", e);
    }
  }

  @Override
  public String toJson() {
    try {
      return OBJECT_MAPPER.writeValueAsString(tenantIsolationGroupConfigDTO);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          "Error in converting TenantIsolationGroupConfigDTO document to json", e);
    }
  }
}
