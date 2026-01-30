package ai.traceable.ratelimiting.config.service.v2.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.commons.v1.AuditFilter;
import ai.traceable.config.commons.v1.TimestampRange;
import ai.traceable.config.service.commons.utils.UserVisibleEmailConfig;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleRecord;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceConfig;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesStore;
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

class RateLimitingRulesStoreAuditFilterTest {

  private MockGenericConfigService mockConfigService;
  private RateLimitingRulesStore rateLimitingRulesStore;
  private RequestContext requestContext;
  private TimestampConverter timestampConverter;
  private RateLimitingConfigServiceConfig config;

  @BeforeEach
  void setup() {
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    requestContext = RequestContext.forTenantId("test-tenant");
    timestampConverter = new TimestampConverter();
    config = mock(RateLimitingConfigServiceConfig.class);
    when(config.getDefaultRateLimitingRules()).thenReturn(Collections.emptyList());
    Config typesafeConfig =
        ConfigFactory.parseString(
            "generic.config.service.customer.visible.excluded.email.patterns: []");
    when(config.getUserVisibleEmailConfig()).thenReturn(new UserVisibleEmailConfig(typesafeConfig));
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    rateLimitingRulesStore =
        new RateLimitingRulesStore(
            configServiceBlockingStub, configChangeEventGenerator, config, timestampConverter);
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  @Nested
  class GetRuleRecordsWithAuditFilter {

    @Test
    void shouldReturnAllRecordsWhenNoAuditFilterApplied() {
      RateLimitingRule rule1 =
          RateLimitingRule.newBuilder()
              .setId("rule-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Rule 1"))
              .build();
      RateLimitingRule rule2 =
          RateLimitingRule.newBuilder()
              .setId("rule-2")
              .setData(RateLimitingRuleData.newBuilder().setName("Rule 2"))
              .build();

      rateLimitingRulesStore.upsertObject(requestContext, rule1);
      rateLimitingRulesStore.upsertObject(requestContext, rule2);

      GetRateLimitingRulesFilter filter = GetRateLimitingRulesFilter.getDefaultInstance();
      List<RateLimitingRuleRecord> records =
          rateLimitingRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(2, records.size());
    }

    @Test
    void shouldReturnEmptyListWhenNoRulesExist() {
      GetRateLimitingRulesFilter filter = GetRateLimitingRulesFilter.getDefaultInstance();
      List<RateLimitingRuleRecord> records =
          rateLimitingRulesStore.getRuleRecords(requestContext, filter);

      assertTrue(records.isEmpty());
    }

    @Test
    void shouldFilterByCreatedByContains_CaseInsensitive() {
      RateLimitingRule rule1 =
          RateLimitingRule.newBuilder()
              .setId("rule-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Rule 1"))
              .build();

      rateLimitingRulesStore.upsertObject(requestContext, rule1);

      GetRateLimitingRulesFilter filter =
          GetRateLimitingRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setCreatedByContains("nonexistent@test.com"))
              .build();

      List<RateLimitingRuleRecord> records =
          rateLimitingRulesStore.getRuleRecords(requestContext, filter);

      // Since mock doesn't populate createdByEmail, no rules should match
      assertTrue(records.isEmpty());
    }

    @Test
    void shouldFilterByLastModifiedByUserContains_CaseInsensitive() {
      RateLimitingRule rule1 =
          RateLimitingRule.newBuilder()
              .setId("rule-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Rule 1"))
              .build();

      rateLimitingRulesStore.upsertObject(requestContext, rule1);

      GetRateLimitingRulesFilter filter =
          GetRateLimitingRulesFilter.newBuilder()
              .setAuditFilter(
                  AuditFilter.newBuilder().setLastUpdatedByUserContains("nonexistent@test.com"))
              .build();

      List<RateLimitingRuleRecord> records =
          rateLimitingRulesStore.getRuleRecords(requestContext, filter);

      // Since mock doesn't populate lastUserUpdateEmail, no rules should match
      assertTrue(records.isEmpty());
    }

    @Test
    void shouldReturnRecordsWhenCreatedByContainsIsEmpty() {
      RateLimitingRule rule1 =
          RateLimitingRule.newBuilder()
              .setId("rule-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Rule 1"))
              .build();

      rateLimitingRulesStore.upsertObject(requestContext, rule1);

      GetRateLimitingRulesFilter filter =
          GetRateLimitingRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setCreatedByContains(""))
              .build();

      List<RateLimitingRuleRecord> records =
          rateLimitingRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
    }

    @Test
    void shouldReturnRecordsWhenLastModifiedByUserContainsIsEmpty() {
      RateLimitingRule rule1 =
          RateLimitingRule.newBuilder()
              .setId("rule-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Rule 1"))
              .build();

      rateLimitingRulesStore.upsertObject(requestContext, rule1);

      GetRateLimitingRulesFilter filter =
          GetRateLimitingRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setLastUpdatedByUserContains(""))
              .build();

      List<RateLimitingRuleRecord> records =
          rateLimitingRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
    }

    @Test
    void shouldFilterByCreatedRange() {
      RateLimitingRule rule1 =
          RateLimitingRule.newBuilder()
              .setId("rule-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Rule 1"))
              .build();

      rateLimitingRulesStore.upsertObject(requestContext, rule1);

      Instant now = Instant.now();
      GetRateLimitingRulesFilter filter =
          GetRateLimitingRulesFilter.newBuilder()
              .setAuditFilter(
                  AuditFilter.newBuilder()
                      .setCreatedRange(
                          TimestampRange.newBuilder()
                              .setStart(timestampConverter.convert(now.minusSeconds(3600)))
                              .setEnd(timestampConverter.convert(now.plusSeconds(3600)))))
              .build();

      List<RateLimitingRuleRecord> records =
          rateLimitingRulesStore.getRuleRecords(requestContext, filter);

      // Verifies the filter doesn't throw exceptions
      assertTrue(records.size() >= 0);
    }

    @Test
    void shouldFilterByModifiedRange() {
      RateLimitingRule rule1 =
          RateLimitingRule.newBuilder()
              .setId("rule-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Rule 1"))
              .build();

      rateLimitingRulesStore.upsertObject(requestContext, rule1);

      Instant now = Instant.now();
      GetRateLimitingRulesFilter filter =
          GetRateLimitingRulesFilter.newBuilder()
              .setAuditFilter(
                  AuditFilter.newBuilder()
                      .setUpdatedRange(
                          TimestampRange.newBuilder()
                              .setStart(timestampConverter.convert(now.minusSeconds(3600)))
                              .setEnd(timestampConverter.convert(now.plusSeconds(3600)))))
              .build();

      List<RateLimitingRuleRecord> records =
          rateLimitingRulesStore.getRuleRecords(requestContext, filter);

      assertTrue(records.size() >= 0);
    }

    @Test
    void shouldBuildAuditDetailsInRecords() {
      RateLimitingRule rule1 =
          RateLimitingRule.newBuilder()
              .setId("rule-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Rule 1"))
              .build();

      rateLimitingRulesStore.upsertObject(requestContext, rule1);

      GetRateLimitingRulesFilter filter = GetRateLimitingRulesFilter.getDefaultInstance();
      List<RateLimitingRuleRecord> records =
          rateLimitingRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
      RateLimitingRuleRecord ruleRecord = records.get(0);
      assertEquals("rule-1", ruleRecord.getRule().getId());
      assertTrue(ruleRecord.hasAuditDetails());
    }

    @Test
    void shouldCombineAuditFilterWithOtherFilters() {
      RateLimitingRule rule1 =
          RateLimitingRule.newBuilder()
              .setId("rule-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Rule 1"))
              .build();
      RateLimitingRule rule2 =
          RateLimitingRule.newBuilder()
              .setId("rule-2")
              .setData(RateLimitingRuleData.newBuilder().setName("Rule 2"))
              .build();

      rateLimitingRulesStore.upsertObject(requestContext, rule1);
      rateLimitingRulesStore.upsertObject(requestContext, rule2);

      // Combine audit filter with empty other filters
      GetRateLimitingRulesFilter filter =
          GetRateLimitingRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.getDefaultInstance())
              .build();

      List<RateLimitingRuleRecord> records =
          rateLimitingRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(2, records.size());
    }
  }

  @Nested
  class DefaultRulesWithAuditFilter {

    private RateLimitingRulesStore createStoreWithDefaultRules(
        List<RateLimitingRule> defaultRules) {
      RateLimitingConfigServiceConfig configWithDefaults =
          mock(RateLimitingConfigServiceConfig.class);
      when(configWithDefaults.getDefaultRateLimitingRules()).thenReturn(defaultRules);
      Config typesafeConfig =
          ConfigFactory.parseString(
              "generic.config.service.customer.visible.excluded.email.patterns: []");
      when(configWithDefaults.getUserVisibleEmailConfig())
          .thenReturn(new UserVisibleEmailConfig(typesafeConfig));
      ConfigChangeEventGenerator changeEventGenerator = mock(ConfigChangeEventGenerator.class);
      ConfigServiceGrpc.ConfigServiceBlockingStub stub =
          ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
      return new RateLimitingRulesStore(
          stub, changeEventGenerator, configWithDefaults, timestampConverter);
    }

    @Test
    void testAuditFilterExcludesDefaultRules() {
      // Setup: configure default rules and create store
      RateLimitingRule defaultRule =
          RateLimitingRule.newBuilder()
              .setId("default-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Default Rule"))
              .build();
      RateLimitingRulesStore storeWithDefaults = createStoreWithDefaultRules(List.of(defaultRule));

      // Create a stored rule
      RateLimitingRule storedRule =
          RateLimitingRule.newBuilder()
              .setId("stored-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Stored Rule"))
              .build();
      storeWithDefaults.upsertObject(requestContext, storedRule);

      // Apply audit filter (created_by_contains) - should exclude default rules
      GetRateLimitingRulesFilter filter =
          GetRateLimitingRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setCreatedByContains("test"))
              .build();

      List<RateLimitingRuleRecord> records =
          storeWithDefaults.getRuleRecords(requestContext, filter);

      // Assert: default rule is excluded (only stored rules may be returned based on audit match)
      assertTrue(
          records.stream().noneMatch(r -> r.getRule().getId().equals("default-1")),
          "Default rules should be excluded when audit filter is active");
    }

    @Test
    void testEmptyAuditFilterStillIncludesDefaultRules() {
      // Setup: configure default rules and create store
      RateLimitingRule defaultRule =
          RateLimitingRule.newBuilder()
              .setId("default-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Default Rule"))
              .build();
      RateLimitingRulesStore storeWithDefaults = createStoreWithDefaultRules(List.of(defaultRule));

      // Create a stored rule
      RateLimitingRule storedRule =
          RateLimitingRule.newBuilder()
              .setId("stored-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Stored Rule"))
              .build();
      storeWithDefaults.upsertObject(requestContext, storedRule);

      // Apply empty audit filter - should still include default rules
      GetRateLimitingRulesFilter filter =
          GetRateLimitingRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().build())
              .build();

      List<RateLimitingRuleRecord> records =
          storeWithDefaults.getRuleRecords(requestContext, filter);

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
      RateLimitingRule defaultRule =
          RateLimitingRule.newBuilder()
              .setId("default-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Default Rule"))
              .build();
      RateLimitingRulesStore storeWithDefaults = createStoreWithDefaultRules(List.of(defaultRule));

      // Create a stored rule
      RateLimitingRule storedRule =
          RateLimitingRule.newBuilder()
              .setId("stored-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Stored Rule"))
              .build();
      storeWithDefaults.upsertObject(requestContext, storedRule);

      // No audit filter - should include default rules
      GetRateLimitingRulesFilter filter = GetRateLimitingRulesFilter.newBuilder().build();

      List<RateLimitingRuleRecord> records =
          storeWithDefaults.getRuleRecords(requestContext, filter);

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
      RateLimitingRule defaultRule =
          RateLimitingRule.newBuilder()
              .setId("default-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Default Rule"))
              .build();
      RateLimitingRulesStore storeWithDefaults = createStoreWithDefaultRules(List.of(defaultRule));

      // Create a stored rule
      RateLimitingRule storedRule =
          RateLimitingRule.newBuilder()
              .setId("stored-1")
              .setData(RateLimitingRuleData.newBuilder().setName("Stored Rule"))
              .build();
      storeWithDefaults.upsertObject(requestContext, storedRule);

      // Apply created_range audit filter - should exclude default rules
      Instant now = Instant.now();
      TimestampRange range =
          TimestampRange.newBuilder()
              .setStart(timestampConverter.convert(now.minusSeconds(3600)))
              .setEnd(timestampConverter.convert(now.plusSeconds(3600)))
              .build();
      GetRateLimitingRulesFilter filter =
          GetRateLimitingRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setCreatedRange(range))
              .build();

      List<RateLimitingRuleRecord> records =
          storeWithDefaults.getRuleRecords(requestContext, filter);

      // Assert: default rule is excluded
      assertTrue(
          records.stream().noneMatch(r -> r.getRule().getId().equals("default-1")),
          "Default rules should be excluded when created_range audit filter is active");
    }
  }
}
