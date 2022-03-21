package ai.traceable.localprocessing.config.service.apinaming.http.utils;

import lombok.Value;

@Value
public class HttpApiNamingConfigInfo {
  ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig httpApiNamingConfig;
  int embryonicThreshold;
}
