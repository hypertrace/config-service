package ai.traceable.genai.system.discovery.config.service.v1.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRule;
import ai.traceable.genai.system.discovery.config.service.v1.validation.GenAiSystemDiscoveryRulesValidator;
import ai.traceable.genai.system.discovery.config.service.v1.validation.GenAiSystemDiscoveryRulesValidatorImpl;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Map;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class GenAiSystemDiscoveryConfigTest {
  private FeatureCachingClient featureCachingClient;
  private GenAiSystemDiscoveryConfig config;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    featureCachingClient = mock(FeatureCachingClient.class);
    config =
        new GenAiSystemDiscoveryConfig(
            new GenAiSystemDiscoveryRulesValidatorImpl(), featureCachingClient);
    requestContext = mock(RequestContext.class);
  }

  @Test
  void getDefaultGenAiSystemDiscoveryRuleMap_featureDisabled_returnsBaseRulesOnly() {
    when(featureCachingClient.isGenAiMlBasedAiClassificationEnabled(requestContext))
        .thenReturn(false);
    Map<String, GenAiSystemDiscoveryRule> rules =
        config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext);
    assertFalse(rules.isEmpty());
    rules
        .values()
        .forEach(
            rule -> {
              assertFalse(rule.getRuleId().isBlank());
              assertFalse(rule.getGenAiSystemDiscoveryRuleData().getName().isBlank());
            });
  }

  @Test
  void getDefaultGenAiSystemDiscoveryRuleMap_featureDisabled_isUnmodifiable() {
    when(featureCachingClient.isGenAiMlBasedAiClassificationEnabled(requestContext))
        .thenReturn(false);
    Map<String, GenAiSystemDiscoveryRule> rules =
        config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext);
    assertThrows(
        UnsupportedOperationException.class,
        () -> rules.put("rule-id", GenAiSystemDiscoveryRule.getDefaultInstance()));
  }

  @Test
  void getDefaultGenAiSystemDiscoveryRuleMap_featureEnabled_returnsMergedRules() {
    when(featureCachingClient.isGenAiMlBasedAiClassificationEnabled(requestContext))
        .thenReturn(false);
    Map<String, GenAiSystemDiscoveryRule> baseRules =
        config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext);

    when(featureCachingClient.isGenAiMlBasedAiClassificationEnabled(requestContext))
        .thenReturn(true);
    Map<String, GenAiSystemDiscoveryRule> mergedRules =
        config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext);

    assertTrue(mergedRules.size() > baseRules.size());
    baseRules.keySet().forEach(ruleId -> assertTrue(mergedRules.containsKey(ruleId)));
  }

  @Test
  void getDefaultGenAiSystemDiscoveryRuleMap_featureEnabled_isUnmodifiable() {
    when(featureCachingClient.isGenAiMlBasedAiClassificationEnabled(requestContext))
        .thenReturn(true);
    Map<String, GenAiSystemDiscoveryRule> rules =
        config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext);
    assertThrows(
        UnsupportedOperationException.class,
        () -> rules.put("rule-id", GenAiSystemDiscoveryRule.getDefaultInstance()));
  }

  @Test
  void getDefaultGenAiSystemDiscoveryRuleMap_featureEnabled_cachesMergedMap() {
    when(featureCachingClient.isGenAiMlBasedAiClassificationEnabled(requestContext))
        .thenReturn(true);
    Map<String, GenAiSystemDiscoveryRule> firstCall =
        config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext);
    Map<String, GenAiSystemDiscoveryRule> secondCall =
        config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext);
    assertSame(firstCall, secondCall);
  }

  @Test
  void getDefaultGenAiSystemDiscoveryRuleMap_validatesRulesOnLoad() {
    when(featureCachingClient.isGenAiMlBasedAiClassificationEnabled(requestContext))
        .thenReturn(true);
    Map<String, GenAiSystemDiscoveryRule> rules =
        config.getDefaultGenAiSystemDiscoveryRuleMap(requestContext);
    rules
        .values()
        .forEach(
            rule -> {
              assertFalse(rule.getRuleId().isBlank());
              assertEquals(rule.getRuleId(), rule.getRuleId());
              assertFalse(rule.getGenAiSystemDiscoveryRuleData().getName().isBlank());
            });
  }

  @Test
  void buildRuleMap_validatorRejectsRule_throwsInvalidArgument() {
    GenAiSystemDiscoveryRulesValidator throwingValidator =
        Mockito.mock(GenAiSystemDiscoveryRulesValidator.class);
    Mockito.doThrow(Status.INVALID_ARGUMENT.asRuntimeException())
        .when(throwingValidator)
        .validateGenAiSystemDiscoveryRule(Mockito.any());
    assertThrows(
        StatusRuntimeException.class,
        () -> new GenAiSystemDiscoveryConfig(throwingValidator, featureCachingClient));
  }
}
