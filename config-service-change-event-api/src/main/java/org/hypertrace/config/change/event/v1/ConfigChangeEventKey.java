package org.hypertrace.config.change.event.v1;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** Key for config change event */
public class ConfigChangeEventKey {
  @JsonProperty String tenantId;

  @JsonProperty String resourceName;

  @JsonProperty String resourceNamespace;

  @JsonProperty String context;

  public ConfigChangeEventKey() {}

  public ConfigChangeEventKey(
          String tenantId, String resourceName, String resourceNamespace, String context) {
    this.tenantId = tenantId;
    this.resourceName = resourceName;
    this.resourceNamespace = resourceNamespace;
    this.context = context;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConfigChangeEventKey that = (ConfigChangeEventKey) o;
    return tenantId.equals(that.tenantId) && resourceName.equals(that.resourceName) && resourceNamespace.equals(that.resourceNamespace) && context.equals(that.context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId, resourceName, resourceNamespace, context);
  }
}
