package ai.traceable.attribute.resolution.config.service.v1.manager;

import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfig;
import ai.traceable.attribute.resolution.config.service.v1.CreateAttributeResolutionConfigRequest;
import ai.traceable.attribute.resolution.config.service.v1.GetAttributeResolutionConfigsFilter;
import ai.traceable.attribute.resolution.config.service.v1.UpdateAttributeResolutionConfigRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface AttributeResolutionConfigManager {

  List<AttributeResolutionConfig> getAttributeResolutionConfigs(
      RequestContext requestContext, GetAttributeResolutionConfigsFilter filter);

  AttributeResolutionConfig createAttributeResolutionConfig(
      RequestContext requestContext, CreateAttributeResolutionConfigRequest request);

  AttributeResolutionConfig updateAttributeResolutionConfig(
      RequestContext requestContext, UpdateAttributeResolutionConfigRequest request);

  void deleteAttributeResolutionConfig(RequestContext requestContext, String id);
}
