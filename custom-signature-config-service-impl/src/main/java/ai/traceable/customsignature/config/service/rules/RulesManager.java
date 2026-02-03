package ai.traceable.customsignature.config.service.rules;

import ai.traceable.customsignature.config.service.v1.BulkUpdateCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRuleRecord;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesManager {
  List<CustomSignatureRule> getCustomSignatureRules(
      RequestContext requestContext, GetRulesFilter filter);

  List<CustomSignatureRuleRecord> getCustomSignatureRuleRecords(
      RequestContext requestContext, GetRulesFilter filter);

  Optional<CustomSignatureRule> createCustomSignatureRule(
      RequestContext requestContext, CreateCustomSignatureRuleRequest createRuleRequest);

  Optional<CustomSignatureRule> updateCustomSignatureRule(
      RequestContext requestContext, CustomSignatureRule customSignatureRule);

  Optional<CustomSignatureRule> deleteCustomSignatureRule(RequestContext requestContext, String id)
      throws InvalidProtocolBufferException;

  void bulkDeleteCustomSignatureRules(RequestContext requestContext, List<String> ids)
      throws InvalidProtocolBufferException;

  void bulkUpdateCustomSignatureRules(
      RequestContext requestContext, BulkUpdateCustomSignatureRulesRequest request);

  default String generateRuleId() {
    return UUID.randomUUID().toString();
  }
}
