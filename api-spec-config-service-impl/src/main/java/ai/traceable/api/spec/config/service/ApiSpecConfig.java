package ai.traceable.api.spec.config.service;

import com.typesafe.config.Config;
import lombok.Value;

@Value
public class ApiSpecConfig {
  private static final String MAX_ALLOWED_SPECS_PER_TENANT = "maxAllowedSpecsPerTenant";
  int maxAllowedSpecsPerTenant;

  ApiSpecConfig(Config config) {
    this.maxAllowedSpecsPerTenant = config.getInt(MAX_ALLOWED_SPECS_PER_TENANT);
  }
}
