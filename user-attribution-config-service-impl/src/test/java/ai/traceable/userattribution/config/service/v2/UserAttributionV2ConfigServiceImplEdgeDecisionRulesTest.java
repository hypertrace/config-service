package ai.traceable.userattribution.config.service.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.ObjectDiffer;
import ai.traceable.config.utils.RankCalculator;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.userattribution.config.service.v2.edge.UserAttributionEdgeDecisionConverter;
import ai.traceable.userattribution.config.service.v2.migration.LegacyUserAttributionRuleTranslatingDao;
import ai.traceable.userattribution.config.service.v2.store.UserAttributionV2RuleGenerator;
import ai.traceable.userattribution.config.service.v2.store.UserAttributionV2RuleStore;
import ai.traceable.userattribution.config.service.v2.validation.UserAttributionV2ConfigRequestValidator;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class UserAttributionV2ConfigServiceImplEdgeDecisionRulesTest {

  @Test
  void getUserAttributionEdgeDecisionRules_returnsConvertedConfigWhenEnabled() {
    FeatureCachingClient featureCachingClient = Mockito.mock(FeatureCachingClient.class);
    UserAttributionV2ConfigRequestValidator validator =
        Mockito.mock(UserAttributionV2ConfigRequestValidator.class);
    UserAttributionV2RuleStore ruleStore = Mockito.mock(UserAttributionV2RuleStore.class);
    UserAttributionV2RuleGenerator ruleGenerator =
        Mockito.mock(UserAttributionV2RuleGenerator.class);
    @SuppressWarnings("unchecked")
    RankCalculator<UserAttributionRule, String> rankCalculator = Mockito.mock(RankCalculator.class);
    ObjectDiffer objectDiffer = Mockito.mock(ObjectDiffer.class);
    LegacyUserAttributionRuleTranslatingDao legacyRuleStore =
        Mockito.mock(LegacyUserAttributionRuleTranslatingDao.class);
    UserAttributionEdgeDecisionConverter edgeDecisionConverter =
        Mockito.mock(UserAttributionEdgeDecisionConverter.class);

    UserAttributionV2ConfigServiceImpl service =
        new UserAttributionV2ConfigServiceImpl(
            featureCachingClient,
            validator,
            ruleStore,
            ruleGenerator,
            rankCalculator,
            objectDiffer,
            legacyRuleStore,
            edgeDecisionConverter);

    GetUserAttributionEdgeDecisionRulesRequest request =
        GetUserAttributionEdgeDecisionRulesRequest.newBuilder().build();

    UserAttributionRule rule =
        UserAttributionRule.newBuilder()
            .setId("r1")
            .setRank(1)
            .setData(UserAttributionRuleData.newBuilder().build())
            .build();

    EdgeDecisionEngineConfig expectedConfig =
        EdgeDecisionEngineConfig.newBuilder().setName("ua-edge-config").build();

    when(ruleStore.getAllConfigData(any(), any())).thenReturn(List.of(rule));
    when(featureCachingClient.isEdgeDecisionEnabledForTenant(any())).thenReturn(true);
    when(edgeDecisionConverter.convert(List.of(rule))).thenReturn(expectedConfig);

    CapturingObserver<GetUserAttributionEdgeDecisionRulesResponse> observer =
        new CapturingObserver<>();

    RequestContext.forTenantId("t1")
        .call(
            () -> {
              service.getUserAttributionEdgeDecisionRules(request, observer);
              return null;
            });

    assertNull(observer.error);
    assertEquals(expectedConfig, observer.value.getEdgeDecisionEngineConfig());
    verify(ruleStore).getAllConfigData(any(), any());
    verify(legacyRuleStore, never()).getUserAttributionRulesFromLegacyStore(any(), any());
  }

  @Test
  void getUserAttributionEdgeDecisionRules_fallsBackToLegacyWhenStoreEmptyAndSourceAllows() {
    FeatureCachingClient featureCachingClient = Mockito.mock(FeatureCachingClient.class);
    UserAttributionV2ConfigRequestValidator validator =
        Mockito.mock(UserAttributionV2ConfigRequestValidator.class);
    UserAttributionV2RuleStore ruleStore = Mockito.mock(UserAttributionV2RuleStore.class);
    UserAttributionV2RuleGenerator ruleGenerator =
        Mockito.mock(UserAttributionV2RuleGenerator.class);
    @SuppressWarnings("unchecked")
    RankCalculator<UserAttributionRule, String> rankCalculator = Mockito.mock(RankCalculator.class);
    ObjectDiffer objectDiffer = Mockito.mock(ObjectDiffer.class);
    LegacyUserAttributionRuleTranslatingDao legacyRuleStore =
        Mockito.mock(LegacyUserAttributionRuleTranslatingDao.class);
    UserAttributionEdgeDecisionConverter edgeDecisionConverter =
        Mockito.mock(UserAttributionEdgeDecisionConverter.class);

    UserAttributionV2ConfigServiceImpl service =
        new UserAttributionV2ConfigServiceImpl(
            featureCachingClient,
            validator,
            ruleStore,
            ruleGenerator,
            rankCalculator,
            objectDiffer,
            legacyRuleStore,
            edgeDecisionConverter);

    GetUserAttributionEdgeDecisionRulesRequest request =
        GetUserAttributionEdgeDecisionRulesRequest.newBuilder().build();

    UserAttributionRule legacyRule =
        UserAttributionRule.newBuilder()
            .setId("legacy")
            .setRank(1)
            .setData(UserAttributionRuleData.newBuilder().build())
            .build();

    EdgeDecisionEngineConfig expectedConfig =
        EdgeDecisionEngineConfig.newBuilder().setName("legacy-edge-config").build();

    when(ruleStore.getAllConfigData(any(), any())).thenReturn(List.of());
    when(legacyRuleStore.getUserAttributionRulesFromLegacyStore(any(), any()))
        .thenReturn(List.of(legacyRule));
    when(featureCachingClient.isEdgeDecisionEnabledForTenant(any())).thenReturn(true);
    when(edgeDecisionConverter.convert(List.of(legacyRule))).thenReturn(expectedConfig);

    CapturingObserver<GetUserAttributionEdgeDecisionRulesResponse> observer =
        new CapturingObserver<>();

    RequestContext.forTenantId("t1")
        .call(
            () -> {
              service.getUserAttributionEdgeDecisionRules(request, observer);
              return null;
            });

    assertNull(observer.error);
    assertEquals(expectedConfig, observer.value.getEdgeDecisionEngineConfig());
    verify(legacyRuleStore).getUserAttributionRulesFromLegacyStore(any(), any());
  }

  @Test
  void getUserAttributionEdgeDecisionRules_returnsDefaultConfigWhenEdgeDecisionDisabled() {
    FeatureCachingClient featureCachingClient = Mockito.mock(FeatureCachingClient.class);
    UserAttributionV2ConfigRequestValidator validator =
        Mockito.mock(UserAttributionV2ConfigRequestValidator.class);
    UserAttributionV2RuleStore ruleStore = Mockito.mock(UserAttributionV2RuleStore.class);
    UserAttributionV2RuleGenerator ruleGenerator =
        Mockito.mock(UserAttributionV2RuleGenerator.class);
    @SuppressWarnings("unchecked")
    RankCalculator<UserAttributionRule, String> rankCalculator = Mockito.mock(RankCalculator.class);
    ObjectDiffer objectDiffer = Mockito.mock(ObjectDiffer.class);
    LegacyUserAttributionRuleTranslatingDao legacyRuleStore =
        Mockito.mock(LegacyUserAttributionRuleTranslatingDao.class);
    UserAttributionEdgeDecisionConverter edgeDecisionConverter =
        Mockito.mock(UserAttributionEdgeDecisionConverter.class);

    UserAttributionV2ConfigServiceImpl service =
        new UserAttributionV2ConfigServiceImpl(
            featureCachingClient,
            validator,
            ruleStore,
            ruleGenerator,
            rankCalculator,
            objectDiffer,
            legacyRuleStore,
            edgeDecisionConverter);

    GetUserAttributionEdgeDecisionRulesRequest request =
        GetUserAttributionEdgeDecisionRulesRequest.newBuilder().build();

    when(ruleStore.getAllConfigData(any(), any())).thenReturn(List.of());
    when(featureCachingClient.isEdgeDecisionEnabledForTenant(any())).thenReturn(false);

    CapturingObserver<GetUserAttributionEdgeDecisionRulesResponse> observer =
        new CapturingObserver<>();

    RequestContext.forTenantId("t1")
        .call(
            () -> {
              service.getUserAttributionEdgeDecisionRules(request, observer);
              return null;
            });

    assertNull(observer.error);
    assertEquals(
        EdgeDecisionEngineConfig.getDefaultInstance(),
        observer.value.getEdgeDecisionEngineConfig());
    verify(edgeDecisionConverter, never()).convert(any());
  }

  private static class CapturingObserver<T> implements StreamObserver<T> {
    private T value;
    private Throwable error;

    @Override
    public void onNext(T value) {
      this.value = value;
    }

    @Override
    public void onError(Throwable t) {
      this.error = t;
    }

    @Override
    public void onCompleted() {}
  }
}
