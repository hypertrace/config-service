package ai.traceable.customsignature.config.service.rules;

import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesManager {

  String CUSTOM_SIGNATURE_RULE_CONFIG_NAMESPACE = "customSignatureRule";
  String CUSTOM_SIGNATURE_RULE_CONFIG_RESOURCE_NAME = "customSignatureRuleConfig";

  List<CustomSignatureRule> getCustomSignatureRules(
      RequestContext requestContext, GetCustomSignatureRulesRequest request);

  Optional<CustomSignatureRule> createCustomSignatureRule(
      RequestContext requestContext, CreateCustomSignatureRuleRequest createRuleRequest);

  Optional<CustomSignatureRule> updateCustomSignatureRule(
      RequestContext requestContext, CustomSignatureRule customSignatureRule);

  boolean deleteCustomSignatureRule(RequestContext requestContext, String id);

  default String generateRuleId() {
    return UUID.randomUUID().toString();
  }
}
