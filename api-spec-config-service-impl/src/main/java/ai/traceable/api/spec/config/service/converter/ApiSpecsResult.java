package ai.traceable.api.spec.config.service.converter;

import ai.traceable.api.spec.config.service.v1.ApiSpec;
import java.util.List;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class ApiSpecsResult {
  List<ApiSpec> specs;
  long totalCount;
}
