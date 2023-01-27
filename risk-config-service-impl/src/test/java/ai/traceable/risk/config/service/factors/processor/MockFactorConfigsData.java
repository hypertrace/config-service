package ai.traceable.risk.config.service.factors.processor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.risk.config.service.processor.RiskConfigConverter;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.CustomizationOptions;
import ai.traceable.risk.config.service.v1.IntOperator;
import ai.traceable.risk.config.service.v1.IntPredicate;
import ai.traceable.risk.config.service.v1.RiskContributorConfigs;
import ai.traceable.risk.config.service.v1.RiskElementConfig;
import ai.traceable.risk.config.service.v1.RiskElementInfo;
import ai.traceable.risk.config.service.v1.RiskElementScoring;
import ai.traceable.risk.config.service.v1.RiskFactor;
import ai.traceable.risk.config.service.v1.RiskFactorConfig;
import ai.traceable.risk.config.service.v1.RiskFactorInfo;
import ai.traceable.risk.config.service.v1.RiskFactorScoring;
import ai.traceable.risk.config.service.v1.RiskFactorType;
import ai.traceable.risk.config.service.v1.StringOperator;
import ai.traceable.risk.config.service.v1.StringPredicate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class MockFactorConfigsData {

  static RiskContributorConfigs mockDefaultRiskContributorConfigs() {
    RiskFactor riskFactor1 = getDefaultCustomTagRiskFactor();
    RiskFactor riskFactor2 = getDefaultMotiveFactor();
    return RiskContributorConfigs.newBuilder()
        .addRiskFactors(riskFactor1)
        .addRiskFactors(riskFactor2)
        .build();
  }

  static RiskContributorConfigs mockDefaultRiskImpactConfigs() {
    RiskFactor riskFactor1 = getDefaultCustomTagRiskFactor();
    RiskFactor riskFactor2 = getDefaultSensitiveDataExposureFactor();
    return RiskContributorConfigs.newBuilder()
        .addRiskFactors(riskFactor1)
        .addRiskFactors(riskFactor2)
        .build();
  }

  static RiskFactor getDefaultCustomTagRiskFactor() {
    return RiskFactor.newBuilder()
        .setIsDefault(true)
        .addCustomizationOptions(CustomizationOptions.CUSTOMIZATION_OPTIONS_ELEMENT_ADD_DELETE)
        .addCustomizationOptions(
            CustomizationOptions.CUSTOMIZATION_OPTIONS_FACTOR_SCORE_CONTRIBUTION)
        .setRiskFactorInfo(
            RiskFactorInfo.newBuilder()
                .setName("custom-tag")
                .setRiskFactorType(RiskFactorType.RISK_FACTOR_TYPE_CUSTOM_TAGS))
        .setRiskFactorConfig(
            RiskFactorConfig.newBuilder()
                .setId("custom-tag")
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("tag-1")
                        .setRiskElementInfo(
                            RiskElementInfo.newBuilder()
                                .setLabelId(
                                    StringPredicate.newBuilder()
                                        .setOperator(StringOperator.STRING_OPERATOR_EQUALS)
                                        .setValue("tag1")))
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3)))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("tag-2")
                        .setRiskElementInfo(
                            RiskElementInfo.newBuilder()
                                .setLabelId(
                                    StringPredicate.newBuilder()
                                        .setOperator(StringOperator.STRING_OPERATOR_EQUALS)
                                        .setValue("tag2")))
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(4))))
        .build();
  }

  static RiskFactor getDefaultSensitiveDataExposureFactor() {
    return RiskFactor.newBuilder()
        .setIsDefault(true)
        .setRiskFactorInfo(
            RiskFactorInfo.newBuilder()
                .setName("sensitive-data-exposure")
                .setRiskFactorType(RiskFactorType.RISK_FACTOR_TYPE_SENSITIVE_DATA_EXPOSURE))
        .setRiskFactorConfig(
            RiskFactorConfig.newBuilder()
                .setId("sensitive-data-exposure")
                .setRiskFactorScoring(RiskFactorScoring.getDefaultInstance())
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("request-has-1-or-more-params")
                        .setRiskElementInfo(
                            RiskElementInfo.newBuilder()
                                .setRequestParamsCount(
                                    IntPredicate.newBuilder()
                                        .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                        .setValue(1)))
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(1)))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("request-has-3-or-more-params")
                        .setRiskElementInfo(
                            RiskElementInfo.newBuilder()
                                .setRequestParamsCount(
                                    IntPredicate.newBuilder()
                                        .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                        .setValue(3)))
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3)))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("request-has-5-or-more-params")
                        .setRiskElementInfo(
                            RiskElementInfo.newBuilder()
                                .setRequestParamsCount(
                                    IntPredicate.newBuilder()
                                        .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                        .setValue(5)))
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(7)))
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("request-has-10-or-more-params")
                        .setRiskElementInfo(
                            RiskElementInfo.newBuilder()
                                .setRequestParamsCount(
                                    IntPredicate.newBuilder()
                                        .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                        .setValue(10)))
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(10))))
        .build();
  }

  static RiskFactorConfig getSensitiveDataExposureFactorConfig(
      boolean disabled, int request5orMoreParamsScore) {
    return RiskFactorConfig.newBuilder()
        .setId("sensitive-data-exposure")
        .setRiskFactorScoring(RiskFactorScoring.newBuilder().setDisabled(disabled))
        .addRiskElementConfigs(
            RiskElementConfig.newBuilder()
                .setId("request-has-1-or-more-params")
                .setRiskElementInfo(
                    RiskElementInfo.newBuilder()
                        .setRequestParamsCount(
                            IntPredicate.newBuilder()
                                .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                .setValue(1)))
                .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(1)))
        .addRiskElementConfigs(
            RiskElementConfig.newBuilder()
                .setId("request-has-3-or-more-params")
                .setRiskElementInfo(
                    RiskElementInfo.newBuilder()
                        .setRequestParamsCount(
                            IntPredicate.newBuilder()
                                .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                .setValue(3)))
                .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(3)))
        .addRiskElementConfigs(
            RiskElementConfig.newBuilder()
                .setId("request-has-5-or-more-params")
                .setRiskElementInfo(
                    RiskElementInfo.newBuilder()
                        .setRequestParamsCount(
                            IntPredicate.newBuilder()
                                .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                .setValue(5)))
                .setRiskElementScoring(
                    RiskElementScoring.newBuilder().setScore(request5orMoreParamsScore)))
        .addRiskElementConfigs(
            RiskElementConfig.newBuilder()
                .setId("request-has-10-or-more-params")
                .setRiskElementInfo(
                    RiskElementInfo.newBuilder()
                        .setRequestParamsCount(
                            IntPredicate.newBuilder()
                                .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                .setValue(10)))
                .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(10)))
        .build();
  }

  static RiskFactor getDefaultMotiveFactor() {
    return RiskFactor.newBuilder()
        .setIsDefault(true)
        .setRiskFactorInfo(
            RiskFactorInfo.newBuilder()
                .setName("motive")
                .setRiskFactorType(RiskFactorType.RISK_FACTOR_TYPE_MOTIVE))
        .setRiskFactorConfig(
            RiskFactorConfig.newBuilder()
                .setId("motive")
                .setRiskFactorScoring(RiskFactorScoring.getDefaultInstance())
                .addRiskElementConfigs(
                    RiskElementConfig.newBuilder()
                        .setId("response-has-pii")
                        .setRiskElementInfo(
                            RiskElementInfo.newBuilder()
                                .setResponsePiiCount(
                                    IntPredicate.newBuilder()
                                        .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                                        .setValue(0)))
                        .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(6))))
        .build();
  }

  static class MockRiskFactorConfigStore extends RiskFactorConfigStore {

    private Map<String, Map<String, ContextualConfigObject<RiskFactorConfig>>> tenantFactorsMap =
        new HashMap<>();

    protected MockRiskFactorConfigStore(
        ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
        RiskConfigConverter<RiskFactorConfig> configConverter,
        RiskConfigUtils<RiskFactorConfig> configUtils,
        ConfigChangeEventGenerator configChangeEventGenerator) {
      super(configServiceBlockingStub, configConverter, configUtils, configChangeEventGenerator);
    }

    @Override
    public List<ContextualConfigObject<RiskFactorConfig>> getAllObjects(
        RequestContext requestContext) {
      String tenantId = requestContext.getTenantId().get();
      return tenantFactorsMap.containsKey(tenantId)
          ? new ArrayList<>(tenantFactorsMap.get(tenantId).values())
          : Collections.emptyList();
    }

    @Override
    public Optional<RiskFactorConfig> getData(RequestContext requestContext, String context) {
      return Optional.ofNullable(tenantFactorsMap.get(requestContext.getTenantId().get()))
          .map(factorsMap -> factorsMap.get(context).getData());
    }

    @Override
    public ContextualConfigObject<RiskFactorConfig> upsertObject(
        RequestContext requestContext, RiskFactorConfig config) {
      String tenantId = requestContext.getTenantId().get();
      if (!tenantFactorsMap.containsKey(tenantId)) {
        tenantFactorsMap.put(tenantId, new HashMap<>());
      }

      ContextualConfigObject<RiskFactorConfig> configObject = mock(ContextualConfigObject.class);
      when(configObject.getData()).thenReturn(config);

      tenantFactorsMap.get(tenantId).put(config.getId(), configObject);
      return configObject;
    }

    @Override
    public Optional<DeletedContextualConfigObject<RiskFactorConfig>> deleteObject(
        RequestContext requestContext, String context) {
      tenantFactorsMap.get(requestContext.getTenantId().get()).remove(context);
      return Optional.of(mock(DeletedContextualConfigObject.class));
    }
  }

  static class MockRiskElementConfigStore extends RiskElementConfigStore {

    private final Map<String, Map<String, ContextualConfigObject<RiskElementConfig>>>
        tenantFactorsMap = new HashMap<>();

    protected MockRiskElementConfigStore(
        ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
        RiskConfigConverter<RiskElementConfig> configConverter,
        RiskConfigUtils<RiskElementConfig> configUtils,
        ConfigChangeEventGenerator configChangeEventGenerator) {
      super(configServiceBlockingStub, configConverter, configUtils, configChangeEventGenerator);
    }

    @Override
    public List<ContextualConfigObject<RiskElementConfig>> getAllObjects(
        RequestContext requestContext) {
      String tenantId = requestContext.getTenantId().get();
      return tenantFactorsMap.containsKey(tenantId)
          ? new ArrayList<>(tenantFactorsMap.get(tenantId).values())
          : Collections.emptyList();
    }

    @Override
    public Optional<RiskElementConfig> getData(RequestContext requestContext, String context) {
      return Optional.ofNullable(tenantFactorsMap.get(requestContext.getTenantId().get()))
          .map(factorsMap -> factorsMap.get(context).getData());
    }

    @Override
    public ContextualConfigObject<RiskElementConfig> upsertObject(
        RequestContext requestContext, RiskElementConfig config) {
      String tenantId = requestContext.getTenantId().get();
      if (!tenantFactorsMap.containsKey(tenantId)) {
        tenantFactorsMap.put(tenantId, new HashMap<>());
      }

      ContextualConfigObject<RiskElementConfig> configObject = mock(ContextualConfigObject.class);
      when(configObject.getData()).thenReturn(config);
      tenantFactorsMap.get(tenantId).put(config.getId(), configObject);
      return configObject;
    }

    @Override
    public Optional<DeletedContextualConfigObject<RiskElementConfig>> deleteObject(
        RequestContext requestContext, String context) {
      tenantFactorsMap.get(requestContext.getTenantId().get()).remove(context);
      return Optional.of(mock(DeletedContextualConfigObject.class));
    }
  }
}
