package ai.traceable.anomaly.config.service.modsec.rules;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.GetDefaultModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetWebAppEvaluationConfigContextRequest;
import com.google.inject.Inject;
import io.grpc.Status;

public class ModsecValidatorImpl implements ModsecValidator {
  private final AnomalyConfigValidator anomalyConfigValidator;

  @Inject
  public ModsecValidatorImpl(AnomalyConfigValidator anomalyConfigValidator) {
    this.anomalyConfigValidator = anomalyConfigValidator;
  }

  @Override
  public Status validate(GetModsecCrsRulesRequest request) {
    for (AnomalySubRuleType type : request.getSubRuleTypesList()) {
      if (type == AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_UNSPECIFIED) {
        return Status.INVALID_ARGUMENT.withDescription("Modsec rule should have a valid type");
      }
    }

    return anomalyConfigValidator.validate(request.getConfigScope());
  }

  @Override
  public Status validate(GetDefaultModsecCrsRulesRequest request) {
    for (AnomalySubRuleType type : request.getSubRuleTypesList()) {
      if (type == AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_UNSPECIFIED) {
        return Status.INVALID_ARGUMENT.withDescription("Modsec rule should have a valid type");
      }
    }
    return Status.OK;
  }

  @Override
  public void validate(GetWebAppEvaluationConfigContextRequest request) {
    validateNonDefaultPresenceOrThrow(
        request, GetWebAppEvaluationConfigContextRequest.RULE_EVALUATION_POINT_FIELD_NUMBER);
  }
}
