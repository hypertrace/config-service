package ai.traceable.span.processing.config.service.apinamingrules;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import java.time.Duration;
import lombok.Getter;

@Getter
public class ApiNamingRulesManagerConfig {
  private static final String API_SPEC_SERVICE_TIMEOUT = "api.spec.config.service.timeout";

  private final Duration apiSpecServiceTimeout;

  @Inject
  ApiNamingRulesManagerConfig(Config config) {
    apiSpecServiceTimeout =
        config.hasPath(API_SPEC_SERVICE_TIMEOUT)
            ? config.getDuration(API_SPEC_SERVICE_TIMEOUT)
            : Duration.ofSeconds(10);
  }
}
