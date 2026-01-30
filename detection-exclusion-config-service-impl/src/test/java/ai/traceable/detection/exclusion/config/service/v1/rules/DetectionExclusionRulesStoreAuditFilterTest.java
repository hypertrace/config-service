package ai.traceable.detection.exclusion.config.service.v1.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.commons.v1.AuditFilter;
import ai.traceable.config.commons.v1.TimestampRange;
import ai.traceable.config.service.commons.utils.UserVisibleEmailConfig;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleRecord;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DetectionExclusionRulesStoreAuditFilterTest {

  private MockGenericConfigService mockConfigService;
  private DetectionExclusionRulesStore detectionExclusionRulesStore;
  private RequestContext requestContext;
  private TimestampConverter timestampConverter;
  private DetectionExclusionConfigServiceConfig config;
  private FeatureCachingClient featureCachingClient;

  @BeforeEach
  void setup() {
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    requestContext = RequestContext.forTenantId("test-tenant");
    timestampConverter = new TimestampConverter();
    config = mock(DetectionExclusionConfigServiceConfig.class);
    when(config.getDefaultDetectionExclusionRules()).thenReturn(Collections.emptyList());
    when(config.getDefaultNewDetectionExclusionRules()).thenReturn(Collections.emptyList());
    featureCachingClient = mock(FeatureCachingClient.class);
    when(featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(any())).thenReturn(false);
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    Config typesafeConfig =
        ConfigFactory.parseString(
            "generic.config.service.customer.visible.excluded.email.patterns: []");
    when(config.getUserVisibleEmailConfig()).thenReturn(new UserVisibleEmailConfig(typesafeConfig));
    detectionExclusionRulesStore =
        new DetectionExclusionRulesStore(
            configServiceBlockingStub,
            configChangeEventGenerator,
            featureCachingClient,
            config,
            new DetectionExclusionAuditHelper(timestampConverter, config));
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  @Nested
  class GetRuleRecordsWithAuditFilter {

    @Test
    void shouldReturnAllRecordsWhenNoAuditFilterApplied() {
      DetectionExclusionRule rule1 =
          DetectionExclusionRule.newBuilder()
              .setId("rule-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Rule 1"))
              .build();
      DetectionExclusionRule rule2 =
          DetectionExclusionRule.newBuilder()
              .setId("rule-2")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Rule 2"))
              .build();

      detectionExclusionRulesStore.upsertObject(requestContext, rule1);
      detectionExclusionRulesStore.upsertObject(requestContext, rule2);

      GetRulesFilter filter = GetRulesFilter.getDefaultInstance();
      List<DetectionExclusionRuleRecord> records =
          detectionExclusionRulesStore.getAllRuleRecords(requestContext, filter);

      assertEquals(2, records.size());
    }

    @Test
    void shouldReturnEmptyListWhenNoRulesExist() {
      GetRulesFilter filter = GetRulesFilter.getDefaultInstance();
      List<DetectionExclusionRuleRecord> records =
          detectionExclusionRulesStore.getAllRuleRecords(requestContext, filter);

      assertTrue(records.isEmpty());
    }

    @Test
    void shouldFilterByCreatedByContains_CaseInsensitive() {
      DetectionExclusionRule rule1 =
          DetectionExclusionRule.newBuilder()
              .setId("rule-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Rule 1"))
              .build();

      detectionExclusionRulesStore.upsertObject(requestContext, rule1);

      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setCreatedByContains("nonexistent@test.com"))
              .build();

      List<DetectionExclusionRuleRecord> records =
          detectionExclusionRulesStore.getAllRuleRecords(requestContext, filter);

      // Since mock doesn't populate createdByEmail, no rules should match
      assertTrue(records.isEmpty());
    }

    @Test
    void shouldFilterByLastModifiedByUserContains_CaseInsensitive() {
      DetectionExclusionRule rule1 =
          DetectionExclusionRule.newBuilder()
              .setId("rule-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Rule 1"))
              .build();

      detectionExclusionRulesStore.upsertObject(requestContext, rule1);

      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(
                  AuditFilter.newBuilder().setLastUpdatedByUserContains("nonexistent@test.com"))
              .build();

      List<DetectionExclusionRuleRecord> records =
          detectionExclusionRulesStore.getAllRuleRecords(requestContext, filter);

      // Since mock doesn't populate lastUserUpdateEmail, no rules should match
      assertTrue(records.isEmpty());
    }

    @Test
    void shouldReturnRecordsWhenCreatedByContainsIsEmpty() {
      DetectionExclusionRule rule1 =
          DetectionExclusionRule.newBuilder()
              .setId("rule-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Rule 1"))
              .build();

      detectionExclusionRulesStore.upsertObject(requestContext, rule1);

      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setCreatedByContains(""))
              .build();

      List<DetectionExclusionRuleRecord> records =
          detectionExclusionRulesStore.getAllRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
    }

    @Test
    void shouldReturnRecordsWhenLastModifiedByUserContainsIsEmpty() {
      DetectionExclusionRule rule1 =
          DetectionExclusionRule.newBuilder()
              .setId("rule-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Rule 1"))
              .build();

      detectionExclusionRulesStore.upsertObject(requestContext, rule1);

      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setLastUpdatedByUserContains(""))
              .build();

      List<DetectionExclusionRuleRecord> records =
          detectionExclusionRulesStore.getAllRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
    }

    @Test
    void shouldFilterByCreatedRange() {
      DetectionExclusionRule rule1 =
          DetectionExclusionRule.newBuilder()
              .setId("rule-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Rule 1"))
              .build();

      detectionExclusionRulesStore.upsertObject(requestContext, rule1);

      Instant now = Instant.now();
      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(
                  AuditFilter.newBuilder()
                      .setCreatedRange(
                          TimestampRange.newBuilder()
                              .setStart(timestampConverter.convert(now.minusSeconds(3600)))
                              .setEnd(timestampConverter.convert(now.plusSeconds(3600)))))
              .build();

      List<DetectionExclusionRuleRecord> records =
          detectionExclusionRulesStore.getAllRuleRecords(requestContext, filter);

      // Rule was created within the time range, so it should be returned
      assertEquals(1, records.size());
      assertEquals("rule-1", records.get(0).getRule().getId());
    }

    @Test
    void shouldExcludeRulesOutsideCreatedRange() {
      DetectionExclusionRule rule1 =
          DetectionExclusionRule.newBuilder()
              .setId("rule-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Rule 1"))
              .build();

      detectionExclusionRulesStore.upsertObject(requestContext, rule1);

      // Set range in the past (before the rule was created)
      Instant pastTime = Instant.now().minusSeconds(7200);
      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(
                  AuditFilter.newBuilder()
                      .setCreatedRange(
                          TimestampRange.newBuilder()
                              .setStart(timestampConverter.convert(pastTime.minusSeconds(3600)))
                              .setEnd(timestampConverter.convert(pastTime))))
              .build();

      List<DetectionExclusionRuleRecord> records =
          detectionExclusionRulesStore.getAllRuleRecords(requestContext, filter);

      // Rule was created after the range, so it should NOT be returned
      assertTrue(records.isEmpty());
    }

    @Test
    void shouldExcludeRulesWhenUpdatedRangeFilterApplied_MockLimitation() {
      // Note: The MockGenericConfigService doesn't populate lastUserUpdateTimestamp in
      // ContextSpecificConfig, so when filtering by updated_range, rules with 0 timestamp
      // are excluded. This test verifies that behavior.
      DetectionExclusionRule rule1 =
          DetectionExclusionRule.newBuilder()
              .setId("rule-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Rule 1"))
              .build();

      detectionExclusionRulesStore.upsertObject(requestContext, rule1);

      Instant now = Instant.now();
      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(
                  AuditFilter.newBuilder()
                      .setUpdatedRange(
                          TimestampRange.newBuilder()
                              .setStart(timestampConverter.convert(now.minusSeconds(3600)))
                              .setEnd(timestampConverter.convert(now.plusSeconds(3600)))))
              .build();

      List<DetectionExclusionRuleRecord> records =
          detectionExclusionRulesStore.getAllRuleRecords(requestContext, filter);

      // Mock doesn't populate lastUserUpdateTimestamp, so rules are excluded
      assertTrue(records.isEmpty());
    }

    @Test
    void shouldBuildAuditDetailsInRecords() {
      DetectionExclusionRule rule1 =
          DetectionExclusionRule.newBuilder()
              .setId("rule-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Rule 1"))
              .build();

      detectionExclusionRulesStore.upsertObject(requestContext, rule1);

      GetRulesFilter filter = GetRulesFilter.getDefaultInstance();
      List<DetectionExclusionRuleRecord> records =
          detectionExclusionRulesStore.getAllRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
      DetectionExclusionRuleRecord ruleRecord = records.get(0);
      assertEquals("rule-1", ruleRecord.getRule().getId());
      assertTrue(ruleRecord.hasAuditDetails());
    }

    @Test
    void shouldCombineAuditFilterWithOtherFilters() {
      DetectionExclusionRule rule1 =
          DetectionExclusionRule.newBuilder()
              .setId("rule-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Rule 1"))
              .build();
      DetectionExclusionRule rule2 =
          DetectionExclusionRule.newBuilder()
              .setId("rule-2")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Rule 2"))
              .build();

      detectionExclusionRulesStore.upsertObject(requestContext, rule1);
      detectionExclusionRulesStore.upsertObject(requestContext, rule2);

      // Combine audit filter with empty other filters
      GetRulesFilter filter =
          GetRulesFilter.newBuilder().setAuditFilter(AuditFilter.getDefaultInstance()).build();

      List<DetectionExclusionRuleRecord> records =
          detectionExclusionRulesStore.getAllRuleRecords(requestContext, filter);

      assertEquals(2, records.size());
    }
  }

  @Nested
  class DefaultRulesWithAuditFilter {

    private DetectionExclusionRulesStore createStoreWithDefaultRules(
        List<DetectionExclusionRule> defaultRules) {
      DetectionExclusionConfigServiceConfig configWithDefaults =
          mock(DetectionExclusionConfigServiceConfig.class);
      when(configWithDefaults.getDefaultDetectionExclusionRules()).thenReturn(defaultRules);
      when(configWithDefaults.getDefaultNewDetectionExclusionRules()).thenReturn(defaultRules);
      ConfigChangeEventGenerator changeEventGenerator = mock(ConfigChangeEventGenerator.class);
      ConfigServiceGrpc.ConfigServiceBlockingStub stub =
          ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
      Config typesafeConfig =
          ConfigFactory.parseString(
              "generic.config.service.customer.visible.excluded.email.patterns: []");
      when(configWithDefaults.getUserVisibleEmailConfig())
          .thenReturn(new UserVisibleEmailConfig(typesafeConfig));
      return new DetectionExclusionRulesStore(
          stub,
          changeEventGenerator,
          featureCachingClient,
          configWithDefaults,
          new DetectionExclusionAuditHelper(timestampConverter, configWithDefaults));
    }

    @Test
    void testAuditFilterExcludesDefaultRules() {
      // Setup: configure default rules and create store
      DetectionExclusionRule defaultRule =
          DetectionExclusionRule.newBuilder()
              .setId("default-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Default Rule"))
              .build();
      DetectionExclusionRulesStore storeWithDefaults =
          createStoreWithDefaultRules(List.of(defaultRule));

      // Create a stored rule
      DetectionExclusionRule storedRule =
          DetectionExclusionRule.newBuilder()
              .setId("stored-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Stored Rule"))
              .build();
      storeWithDefaults.upsertObject(requestContext, storedRule);

      // Apply audit filter (created_by_contains) - should exclude default rules
      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setCreatedByContains("test"))
              .build();

      List<DetectionExclusionRuleRecord> records =
          storeWithDefaults.getAllRuleRecords(requestContext, filter);

      // Assert: default rule is excluded (only stored rules may be returned based on audit match)
      assertTrue(
          records.stream().noneMatch(r -> r.getRule().getId().equals("default-1")),
          "Default rules should be excluded when audit filter is active");
    }

    @Test
    void testEmptyAuditFilterStillIncludesDefaultRules() {
      // Setup: configure default rules and create store
      DetectionExclusionRule defaultRule =
          DetectionExclusionRule.newBuilder()
              .setId("default-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Default Rule"))
              .build();
      DetectionExclusionRulesStore storeWithDefaults =
          createStoreWithDefaultRules(List.of(defaultRule));

      // Create a stored rule
      DetectionExclusionRule storedRule =
          DetectionExclusionRule.newBuilder()
              .setId("stored-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Stored Rule"))
              .build();
      storeWithDefaults.upsertObject(requestContext, storedRule);

      // Apply empty audit filter - should still include default rules
      GetRulesFilter filter =
          GetRulesFilter.newBuilder().setAuditFilter(AuditFilter.newBuilder().build()).build();

      List<DetectionExclusionRuleRecord> records =
          storeWithDefaults.getAllRuleRecords(requestContext, filter);

      // Assert: both stored and default rules are returned
      assertEquals(2, records.size());
      assertTrue(
          records.stream().anyMatch(r -> r.getRule().getId().equals("default-1")),
          "Default rules should be included when audit filter is empty");
      assertTrue(
          records.stream().anyMatch(r -> r.getRule().getId().equals("stored-1")),
          "Stored rules should be included");
    }

    @Test
    void testNoAuditFilterIncludesDefaultRules() {
      // Setup: configure default rules and create store
      DetectionExclusionRule defaultRule =
          DetectionExclusionRule.newBuilder()
              .setId("default-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Default Rule"))
              .build();
      DetectionExclusionRulesStore storeWithDefaults =
          createStoreWithDefaultRules(List.of(defaultRule));

      // Create a stored rule
      DetectionExclusionRule storedRule =
          DetectionExclusionRule.newBuilder()
              .setId("stored-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Stored Rule"))
              .build();
      storeWithDefaults.upsertObject(requestContext, storedRule);

      // No audit filter - should include default rules
      GetRulesFilter filter = GetRulesFilter.newBuilder().build();

      List<DetectionExclusionRuleRecord> records =
          storeWithDefaults.getAllRuleRecords(requestContext, filter);

      // Assert: both stored and default rules are returned
      assertEquals(2, records.size());
      assertTrue(
          records.stream().anyMatch(r -> r.getRule().getId().equals("default-1")),
          "Default rules should be included when no audit filter");
      assertTrue(
          records.stream().anyMatch(r -> r.getRule().getId().equals("stored-1")),
          "Stored rules should be included");
    }

    @Test
    void testCreatedRangeAuditFilterExcludesDefaultRules() {
      // Setup: configure default rules and create store
      DetectionExclusionRule defaultRule =
          DetectionExclusionRule.newBuilder()
              .setId("default-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Default Rule"))
              .build();
      DetectionExclusionRulesStore storeWithDefaults =
          createStoreWithDefaultRules(List.of(defaultRule));

      // Create a stored rule
      DetectionExclusionRule storedRule =
          DetectionExclusionRule.newBuilder()
              .setId("stored-1")
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("Stored Rule"))
              .build();
      storeWithDefaults.upsertObject(requestContext, storedRule);

      // Apply created_range audit filter - should exclude default rules
      Instant now = Instant.now();
      TimestampRange range =
          TimestampRange.newBuilder()
              .setStart(timestampConverter.convert(now.minusSeconds(3600)))
              .setEnd(timestampConverter.convert(now.plusSeconds(3600)))
              .build();
      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setCreatedRange(range))
              .build();

      List<DetectionExclusionRuleRecord> records =
          storeWithDefaults.getAllRuleRecords(requestContext, filter);

      // Assert: default rule is excluded
      assertTrue(
          records.stream().noneMatch(r -> r.getRule().getId().equals("default-1")),
          "Default rules should be excluded when created_range audit filter is active");
    }
  }
}
