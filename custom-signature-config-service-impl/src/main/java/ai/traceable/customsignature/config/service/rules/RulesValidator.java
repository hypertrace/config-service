package ai.traceable.customsignature.config.service.rules;

import ai.traceable.customsignature.config.service.v1.BulkDeleteCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEdgeDecisionRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import io.grpc.Status;

public interface RulesValidator {
  Status validate(CreateCustomSignatureRuleRequest request);

  Status validate(UpdateCustomSignatureRuleRequest request);

  Status validate(DeleteCustomSignatureRuleRequest request);

  Status validate(BulkDeleteCustomSignatureRulesRequest request);

  Status validate(GetCustomSignatureEdgeDecisionRulesRequest request);

  Status validate(GetCustomSignatureRulesRequest request);

  Status validate(GetCustomSignatureModsecRulesRequest request);
}
