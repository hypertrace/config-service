package ai.traceable.genai.system.discovery.config.service.v1.validation;

import ai.traceable.genai.system.discovery.config.service.v1.CreateGenAiSystemDiscoveryRuleRequest;
import ai.traceable.genai.system.discovery.config.service.v1.DeleteGenAiSystemDiscoveryRuleRequest;
import ai.traceable.genai.system.discovery.config.service.v1.GetGenAiSystemDiscoveryRulesRequest;
import ai.traceable.genai.system.discovery.config.service.v1.UpdateGenAiSystemDiscoveryRuleRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface GenAiSystemDiscoveryRulesValidator {

  void validateOrThrow(RequestContext requestContext, GetGenAiSystemDiscoveryRulesRequest request);

  void validateOrThrow(
      RequestContext requestContext, CreateGenAiSystemDiscoveryRuleRequest request);

  void validateOrThrow(
      RequestContext requestContext, UpdateGenAiSystemDiscoveryRuleRequest request);

  void validateOrThrow(
      RequestContext requestContext, DeleteGenAiSystemDiscoveryRuleRequest request);
}
