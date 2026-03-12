package ai.traceable.customsignature.config.service.rules;

import ai.traceable.customsignature.config.service.v1.BulkDeleteCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.BulkUpdateCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEdgeDecisionRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;

public interface RulesValidator {
  void validate(GetCustomSignatureEvaluationConfigContextRequest request);

  void validate(CreateCustomSignatureRuleRequest request);

  void validate(UpdateCustomSignatureRuleRequest request);

  void validate(DeleteCustomSignatureRuleRequest request);

  void validate(GetCustomSignatureEdgeDecisionRulesRequest request);

  void validate(GetCustomSignatureRulesRequest request);

  void validate(GetCustomSignatureModsecRulesRequest request);

  void validate(BulkDeleteCustomSignatureRulesRequest request);

  void validate(BulkUpdateCustomSignatureRulesRequest request);
}
