package ai.traceable.attribute.resolution.info;

import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfig;
import java.util.Map;
import lombok.Value;

@Value
public class AttributeResolutionConfigInfo {
  Map<String, AttributeResolutionConfig> idToAttributeResolutionConfigMap;
}
