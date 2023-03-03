package ai.traceable.anomaly.config.service.trainer.trainingconfig;

import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.MetadataTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.MultiValuedStringParamRule;
import ai.traceable.anomaly.config.service.v1.trainer.ObjectBolaTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.SessionTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig;
import ai.traceable.config.utils.RegexValidator;
import io.grpc.Status;
import java.util.function.Predicate;

class TrainingConfigRegexValidator {

  public Status validateMetadataTrainingConfigRegex(MetadataTrainingConfig metadataTrainingConfig) {

    Status status = Status.OK;
    switch (metadataTrainingConfig.getConfigCase()) {
      case JWT_PARAMS:
        status =
            metadataTrainingConfig.getJwtParams().getIncludeParamRegexes().getValuesList().stream()
                .map(RegexValidator::validate)
                .filter(Predicate.not(Status::isOk))
                .findFirst()
                .orElse(Status.OK);
        break;
      default:
    }
    return status;
  }

  public Status validateVulnerabilityTrainingConfigRegex(
      VulnerabilityTrainingConfig vulnerabilityTrainingConfig) {
    Status status = Status.OK;
    switch (vulnerabilityTrainingConfig.getConfigCase()) {
      case ENUMERABLE_PARAM:
        if (vulnerabilityTrainingConfig.getEnumerableParam().hasIncludeParamRegex()) {
          status =
              RegexValidator.validate(
                  vulnerabilityTrainingConfig.getEnumerableParam().getIncludeParamRegex());
        }
        break;
      default:
    }
    return status;
  }

  public Status validateSessionTrainingConfigRegex(SessionTrainingConfig sessionTrainingConfig) {
    Status status = Status.OK;
    switch (sessionTrainingConfig.getConfigCase()) {
      case OBJECT_BOLA:
        ObjectBolaTrainingConfig objectBolaTrainingConfig = sessionTrainingConfig.getObjectBola();
        status =
            RegexValidator.validate(
                objectBolaTrainingConfig
                    .getParamSusceptibilityConfig()
                    .getRequestParamValueRegex());
        if (!status.isOk()) {
          return status;
        }
        status =
            objectBolaTrainingConfig.getMultiValuedStringParamRules().getRulesList().stream()
                .map(rule -> RegexValidator.validate(rule.getKeyRegex()))
                .filter(Predicate.not(Status::isOk))
                .findFirst()
                .orElse(Status.OK);
        if (!status.isOk()) {
          return status;
        }
        status =
            objectBolaTrainingConfig.getMultiValuedStringParamRules().getRulesList().stream()
                .filter(MultiValuedStringParamRule::hasValueRegex)
                .map(rule -> RegexValidator.validate(rule.getValueRegex()))
                .filter(Predicate.not(Status::isOk))
                .findFirst()
                .orElse(Status.OK);
        break;
      default:
    }
    return status;
  }

  Status validateApiNamingTrainingConfigRegex(ApiNamingTrainingConfig apiNamingTrainingConfig) {
    Status status = Status.OK;
    switch (apiNamingTrainingConfig.getConfigCase()) {
      case URL_FILTER_CONFIG:
        status =
            apiNamingTrainingConfig
                .getUrlFilterConfig()
                .getUrlRejectRegexPatterns()
                .getValuesList()
                .stream()
                .map(RegexValidator::validate)
                .filter(Predicate.not(Status::isOk))
                .findFirst()
                .orElse(Status.OK);
        break;

      case TRIE_MODEL_TRAINING_CONFIG:
        status =
            apiNamingTrainingConfig
                .getTrieModelTrainingConfig()
                .getAllowRegexList()
                .getValuesList()
                .stream()
                .map(RegexValidator::validate)
                .filter(Predicate.not(Status::isOk))
                .findFirst()
                .orElse(Status.OK);
        break;

      case CUSTOM_RULES_LIST_CONFIG:
        status =
            apiNamingTrainingConfig.getCustomRulesListConfig().getCustomRulesConfigList().stream()
                .map(rule -> RegexValidator.validate(rule.getRegex()))
                .filter(Predicate.not(Status::isOk))
                .findFirst()
                .orElse(Status.OK);
        break;

      case REJECT_FILTER_CONFIG:
        status =
            apiNamingTrainingConfig
                .getRejectFilterConfig()
                .getUrlPathFilterConfig()
                .getUrlPathRegexPatterns()
                .getValuesList()
                .stream()
                .map(RegexValidator::validate)
                .filter(Predicate.not(Status::isOk))
                .findFirst()
                .orElse(Status.OK);
        break;
      default:
    }
    return status;
  }
}
