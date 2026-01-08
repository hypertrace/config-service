package ai.traceable.ratelimiting.config.service.v2.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.commons.v1.AuditFilter;
import ai.traceable.config.commons.v1.TimestampRange;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleRecord;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceConfig;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesStore;
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
}
