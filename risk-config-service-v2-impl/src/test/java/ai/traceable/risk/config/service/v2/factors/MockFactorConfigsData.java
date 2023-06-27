package ai.traceable.risk.config.service.v2.factors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.risk.config.service.v2.EnumOperator;
import ai.traceable.risk.config.service.v2.PathParamTypePredicate;
import ai.traceable.risk.config.service.v2.PathParamTypeValue;
import ai.traceable.risk.config.service.v2.ResponseSensitivityPredicate;
import ai.traceable.risk.config.service.v2.ResponseSensitivityValue;
import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskConfigConverter;
import ai.traceable.risk.config.service.v2.RiskConfigIdGenerator;
import ai.traceable.risk.config.service.v2.RiskContributorCategory;
import ai.traceable.risk.config.service.v2.RiskContributorConfigs;
import ai.traceable.risk.config.service.v2.RiskElementConfig;
import ai.traceable.risk.config.service.v2.RiskElementPredicate;
import ai.traceable.risk.config.service.v2.RiskElementScoring;
import ai.traceable.risk.config.service.v2.RiskFactor;
import ai.traceable.risk.config.service.v2.RiskFactorCategory;
import ai.traceable.risk.config.service.v2.RiskFactorConfig;
import ai.traceable.risk.config.service.v2.RiskFactorInfo;
import ai.traceable.risk.config.service.v2.StringPredicate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class MockFactorConfigsData {

  public static RiskContributorConfigs getDefaultRiskContributorConfigs() {
    RiskFactor defaultResourceDiscoveryFactor = getDefaultResourceDiscoveryFactor();
    RiskFactor defaultSensitiveDataExposureFactor = getDefaultSensitiveDataExposureFactor();
    RiskFactor defaultLabelsFactor = getDefaultLabelsFactor();
    return RiskContributorConfigs.newBuilder()
        .addRiskFactors(defaultResourceDiscoveryFactor)
        .addRiskFactors(defaultSensitiveDataExposureFactor)
        .addRiskFactors(defaultLabelsFactor)
        .build();
  }

  private static RiskFactor getDefaultLabelsFactor() {
    return RiskFactor.newBuilder()
        .setRiskFactorInfo(
            RiskFactorInfo.newBuilder()
                .setIsDefault(true)
                .setRiskContributorCategory(
                    RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_IMPACT))
        .setRiskFactorConfig(
            RiskFactorConfig.newBuilder()
                .setDisabled(false)
                .setRiskFactorCategory(RiskFactorCategory.RISK_FACTOR_CATEGORY_LABELS)
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("critical")
                        .setRiskElementPredicate(
                            RiskElementPredicate.newBuilder()
                                .setLabelId(StringPredicate.newBuilder().setValue("Critical")))
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(5)))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("sensitive")
                        .setRiskElementPredicate(
                            RiskElementPredicate.newBuilder()
                                .setLabelId(StringPredicate.newBuilder().setValue("Sensitive")))
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3)))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("sentry")
                        .setRiskElementPredicate(
                            RiskElementPredicate.newBuilder()
                                .setLabelId(StringPredicate.newBuilder().setValue("Sentry")))
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(9))))
        .build();
  }

  public static RiskFactor getDefaultSensitiveDataExposureFactor() {
    return RiskFactor.newBuilder()
        .setRiskFactorInfo(
            RiskFactorInfo.newBuilder()
                .setRiskContributorCategory(
                    RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_IMPACT)
                .setIsDefault(true))
        .setRiskFactorConfig(
            RiskFactorConfig.newBuilder()
                .setDisabled(false)
                .setRiskFactorCategory(
                    RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE)
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("responseSensitivityCritical")
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(8))
                        .setRiskElementPredicate(
                            RiskElementPredicate.newBuilder()
                                .setResponseSensitivity(
                                    ResponseSensitivityPredicate.newBuilder()
                                        .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                                        .setValue(
                                            ResponseSensitivityValue
                                                .RESPONSE_SENSITIVITY_VALUE_CRITICAL))))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("responseSensitivityHigh")
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(6))
                        .setRiskElementPredicate(
                            RiskElementPredicate.newBuilder()
                                .setResponseSensitivity(
                                    ResponseSensitivityPredicate.newBuilder()
                                        .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                                        .setValue(
                                            ResponseSensitivityValue
                                                .RESPONSE_SENSITIVITY_VALUE_HIGH))))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("responseSensitivityMedium")
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(8))
                        .setRiskElementPredicate(
                            RiskElementPredicate.newBuilder()
                                .setResponseSensitivity(
                                    ResponseSensitivityPredicate.newBuilder()
                                        .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                                        .setValue(
                                            ResponseSensitivityValue
                                                .RESPONSE_SENSITIVITY_VALUE_MEDIUM))))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("responseSensitivityLow")
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(8))
                        .setRiskElementPredicate(
                            RiskElementPredicate.newBuilder()
                                .setResponseSensitivity(
                                    ResponseSensitivityPredicate.newBuilder()
                                        .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                                        .setValue(
                                            ResponseSensitivityValue
                                                .RESPONSE_SENSITIVITY_VALUE_LOW)))))
        .build();
  }

  public static RiskFactor
      getDefaultSensitiveDataExposureFactorWithChangedResponseSensitivityCriticalScore() {
    return RiskFactor.newBuilder()
        .setRiskFactorInfo(
            RiskFactorInfo.newBuilder()
                .setRiskContributorCategory(
                    RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_IMPACT)
                .setIsDefault(true))
        .setRiskFactorConfig(
            RiskFactorConfig.newBuilder()
                .setDisabled(false)
                .setRiskFactorCategory(
                    RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE)
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("responseSensitivityCritical")
                        .setDisabled(true)
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(9))
                        .setRiskElementPredicate(
                            RiskElementPredicate.newBuilder()
                                .setResponseSensitivity(
                                    ResponseSensitivityPredicate.newBuilder()
                                        .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                                        .setValue(
                                            ResponseSensitivityValue
                                                .RESPONSE_SENSITIVITY_VALUE_CRITICAL))))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("responseSensitivityHigh")
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(6))
                        .setRiskElementPredicate(
                            RiskElementPredicate.newBuilder()
                                .setResponseSensitivity(
                                    ResponseSensitivityPredicate.newBuilder()
                                        .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                                        .setValue(
                                            ResponseSensitivityValue
                                                .RESPONSE_SENSITIVITY_VALUE_HIGH))))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("responseSensitivityMedium")
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(8))
                        .setRiskElementPredicate(
                            RiskElementPredicate.newBuilder()
                                .setResponseSensitivity(
                                    ResponseSensitivityPredicate.newBuilder()
                                        .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                                        .setValue(
                                            ResponseSensitivityValue
                                                .RESPONSE_SENSITIVITY_VALUE_MEDIUM))))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("responseSensitivityLow")
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(8))
                        .setRiskElementPredicate(
                            RiskElementPredicate.newBuilder()
                                .setResponseSensitivity(
                                    ResponseSensitivityPredicate.newBuilder()
                                        .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                                        .setValue(
                                            ResponseSensitivityValue
                                                .RESPONSE_SENSITIVITY_VALUE_LOW)))))
        .build();
  }

  public static RiskFactor buildRiskFactorWithoutScope(RiskFactor riskFactor) {
    return RiskFactor.newBuilder()
        .setRiskFactorInfo(riskFactor.getRiskFactorInfo())
        .setRiskFactorConfig(
            RiskFactorConfig.newBuilder()
                .addAllRiskElementConfigs(
                    riskFactor.getRiskFactorConfig().getRiskElementConfigsList())
                .setRiskFactorCategory(riskFactor.getRiskFactorConfig().getRiskFactorCategory())
                .setDisabled(riskFactor.getRiskFactorConfig().getDisabled()))
        .build();
  }

  public static RiskFactor getDefaultResourceDiscoveryFactor() {
    return RiskFactor.newBuilder()
        .setRiskFactorInfo(
            RiskFactorInfo.newBuilder()
                .setRiskContributorCategory(
                    RiskContributorCategory.RISK_CONTRIBUTOR_CATEGORY_LIKELIHOOD)
                .setIsDefault(true))
        .setRiskFactorConfig(
            RiskFactorConfig.newBuilder()
                .setDisabled(false)
                .setRiskFactorCategory(
                    RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_RESOURCE_DISCOVERY)
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("pathParamTypeNone")
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(6))
                        .setRiskElementPredicate(
                            RiskElementPredicate.newBuilder()
                                .setPathParamType(
                                    PathParamTypePredicate.newBuilder()
                                        .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                                        .setValue(PathParamTypeValue.PATH_PARAM_TYPE_VALUE_NONE))))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("pathParamTypeDigit")
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(8))
                        .setRiskElementPredicate(
                            RiskElementPredicate.newBuilder()
                                .setPathParamType(
                                    PathParamTypePredicate.newBuilder()
                                        .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                                        .setValue(PathParamTypeValue.PATH_PARAM_TYPE_VALUE_DIGIT))))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("pathParamTypeString")
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(4))
                        .setRiskElementPredicate(
                            RiskElementPredicate.newBuilder()
                                .setPathParamType(
                                    PathParamTypePredicate.newBuilder()
                                        .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                                        .setValue(
                                            PathParamTypeValue.PATH_PARAM_TYPE_VALUE_STRING)))))
        .build();
  }

  public static class MockRiskFactorConfigStore extends RiskFactorConfigStore {

    private final Map<String, Map<String, ContextualConfigObject<RiskFactorConfig>>>
        tenantFactorsMap = new HashMap<>();
    private final RiskConfigIdGenerator configIdGenerator;

    public MockRiskFactorConfigStore(
        ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
        RiskConfigConverter<RiskFactorConfig> configConverter,
        RiskConfigBuilder<RiskFactorConfig> configBuilder,
        ConfigChangeEventGenerator configChangeEventGenerator,
        RiskConfigIdGenerator configIdGenerator) {
      super(
          configServiceBlockingStub,
          configConverter,
          configBuilder,
          configChangeEventGenerator,
          configIdGenerator);
      this.configIdGenerator = configIdGenerator;
    }

    @Override
    public Optional<RiskFactorConfig> getData(RequestContext requestContext, String id) {
      return Optional.ofNullable(tenantFactorsMap.get(requestContext.getTenantId().get()))
          .map(valuesMap -> valuesMap.get(id))
          .map(ConfigObject::getData);
    }

    @Override
    public ContextualConfigObject<RiskFactorConfig> upsertObject(
        RequestContext requestContext, RiskFactorConfig riskFactorConfig) {
      String tenantId = requestContext.getTenantId().get();
      if (!tenantFactorsMap.containsKey(tenantId)) {
        tenantFactorsMap.put(tenantId, new HashMap<>());
      }

      ContextualConfigObject<RiskFactorConfig> configObject = mock(ContextualConfigObject.class);
      when(configObject.getData()).thenReturn(riskFactorConfig);

      tenantFactorsMap
          .get(tenantId)
          .put(
              configIdGenerator.generateId(
                  riskFactorConfig.getRiskFactorCategory().name(),
                  riskFactorConfig.getRiskConfigScope()),
              configObject);
      return configObject;
    }

    @Override
    public Optional<DeletedContextualConfigObject<RiskFactorConfig>> deleteObject(
        RequestContext requestContext, String id) {
      requestContext.getTenantId().map(tenantFactorsMap::get).ifPresent(map -> map.remove(id));
      return Optional.of(mock(DeletedContextualConfigObject.class));
    }
  }
}
