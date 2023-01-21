package org.hypertrace.tenantisolation.config.service.store;

import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;

@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
@Builder(toBuilder = true)
public class TenantIsolationGroupConfigDTO {
  private String groupName;
  private List<String> members;
  private int weight;
}
