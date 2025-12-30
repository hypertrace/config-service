package ai.traceable.application.grouping.config.service.validations;

import ai.traceable.application.grouping.config.service.v1.CreateApplicationGroupingRuleConfigRequest;
import ai.traceable.application.grouping.config.service.v1.DeleteApplicationGroupingRuleConfigsRequest;
import ai.traceable.application.grouping.config.service.v1.GetAllApplicationGroupingRuleConfigsRequest;
import ai.traceable.application.grouping.config.service.v1.UpdateApplicationGroupingRuleConfigRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ApplicationGroupingConfigServiceRequestValidator {
  void validateOrThrow(
      RequestContext requestContext, GetAllApplicationGroupingRuleConfigsRequest request);

  void validateOrThrow(
      RequestContext requestContext, DeleteApplicationGroupingRuleConfigsRequest request);

  void validateOrThrow(
      RequestContext requestContext, UpdateApplicationGroupingRuleConfigRequest request);

  void validateOrThrow(
      RequestContext requestContext, CreateApplicationGroupingRuleConfigRequest request);
}
