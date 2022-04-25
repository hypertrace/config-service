package ai.traceable.localprocessing.config.service.apinaming.http.utils;

import ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig;
import ai.traceable.platform.apientity.TrieNodeType;
import java.util.EnumMap;
import java.util.List;
import lombok.Value;

@Value
public class HttpApiNamingConfigInfo {
  HttpApiNamingConfig httpApiNamingConfig;
  int embryonicThreshold;
  EnumMap<TrieNodeType, String> wildcardConfigMap;
  List<String> segmentWhitelistRegexes;
  List<String> extensions;
}
