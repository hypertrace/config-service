package ai.traceable.external.data.classification.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

class ExternalDataClassificationConfigRequestValidator {
  public void validateOrThrow(
      RequestContext requestContext, GetDataClassificationConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
  }
}
