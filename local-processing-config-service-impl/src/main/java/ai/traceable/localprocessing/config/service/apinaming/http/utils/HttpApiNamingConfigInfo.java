package ai.traceable.localprocessing.config.service.apinaming.http.utils;

import ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig;
import ai.traceable.platform.apientity.http.model.NodeType;
import java.util.EnumMap;
import java.util.List;
import java.util.Set;
import lombok.Value;

@Value
public class HttpApiNamingConfigInfo {
  HttpApiNamingConfig httpApiNamingConfig;
  int embryonicThreshold;
  EnumMap<NodeType, String> wildcardConfigMap;
  List<String> segmentWhitelistRegexes;
  List<String> extensions;
  Set<String> idEnums;
  int maxNumberOfTriePaths;
}
