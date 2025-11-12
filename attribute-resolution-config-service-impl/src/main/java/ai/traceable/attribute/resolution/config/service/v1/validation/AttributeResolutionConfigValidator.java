package ai.traceable.attribute.resolution.config.service.v1.validation;

import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfigData;
import ai.traceable.attribute.resolution.config.service.v1.CreateAttributeResolutionConfigRequest;
import ai.traceable.attribute.resolution.config.service.v1.DeleteAttributeResolutionConfigRequest;
import ai.traceable.attribute.resolution.config.service.v1.GetAttributeResolutionConfigsRequest;
import ai.traceable.attribute.resolution.config.service.v1.UpdateAttributeResolutionConfigRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface AttributeResolutionConfigValidator {
  void validateOrThrow(RequestContext context, GetAttributeResolutionConfigsRequest request);

  void validateOrThrow(RequestContext context, CreateAttributeResolutionConfigRequest request);

  void validateOrThrow(RequestContext context, UpdateAttributeResolutionConfigRequest request);

  void validateOrThrow(RequestContext context, DeleteAttributeResolutionConfigRequest request);

  void validateAttributeResolutionConfigData(AttributeResolutionConfigData data);
}
