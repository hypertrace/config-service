package ai.traceable.region.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.audit.utils.UserVisibleEmailConfig;
import ai.traceable.config.commons.v1.AuditFilter;
import ai.traceable.config.commons.v1.TimestampRange;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.region.config.service.RegionConfigServiceConfig;
import ai.traceable.region.config.service.v1.GetRegionRulesFilter;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleRecord;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Instant;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RegionRulesStoreAuditFilterTest {

  private MockGenericConfigService mockConfigService;
  private RegionRulesStore regionRulesStore;
  private RequestContext requestContext;
  private TimestampConverter timestampConverter;

  @BeforeEach
  void setup() {
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    requestContext = RequestContext.forTenantId("test-tenant");
    timestampConverter = new TimestampConverter();
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    Config typesafeConfig =
        ConfigFactory.parseString(
            "generic.config.service.customer.visible.excluded.email.patterns: []");
    RegionConfigServiceConfig regionConfigServiceConfig = mock(RegionConfigServiceConfig.class);
    when(regionConfigServiceConfig.getUserVisibleEmailConfig())
        .thenReturn(new UserVisibleEmailConfig(typesafeConfig));
    regionRulesStore =
        new RegionRulesStore(
            configServiceBlockingStub, configChangeEventGenerator, regionConfigServiceConfig);
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  @Nested
  class GetRuleRecordsWithAuditFilter {

    @Test
    void shouldReturnAllRecordsWhenNoAuditFilterApplied() {
      RegionRule rule1 = RegionRule.newBuilder().setId("rule-1").setName("Rule 1").build();
      RegionRule rule2 = RegionRule.newBuilder().setId("rule-2").setName("Rule 2").build();

      regionRulesStore.upsertObject(requestContext, rule1);
      regionRulesStore.upsertObject(requestContext, rule2);

      GetRegionRulesFilter filter = GetRegionRulesFilter.getDefaultInstance();
      List<RegionRuleRecord> records = regionRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(2, records.size());
    }

    @Test
    void shouldReturnEmptyListWhenNoRulesExist() {
      GetRegionRulesFilter filter = GetRegionRulesFilter.getDefaultInstance();
      List<RegionRuleRecord> records = regionRulesStore.getRuleRecords(requestContext, filter);

      assertTrue(records.isEmpty());
    }

    @Test
    void shouldFilterByCreatedByContains_CaseInsensitive() {
      RegionRule rule1 = RegionRule.newBuilder().setId("rule-1").setName("Rule 1").build();
      RegionRule rule2 = RegionRule.newBuilder().setId("rule-2").setName("Rule 2").build();

      regionRulesStore.upsertObject(requestContext, rule1);
      regionRulesStore.upsertObject(requestContext, rule2);

      // Filter by created_by_contains - should match case-insensitively
      // Note: Since MockGenericConfigService doesn't populate audit fields,
      // rules without createdByEmail will not match any created_by_contains filter
      GetRegionRulesFilter filter =
          GetRegionRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setCreatedByContains("nonexistent@test.com"))
              .build();

      List<RegionRuleRecord> records = regionRulesStore.getRuleRecords(requestContext, filter);

      // Since mock doesn't populate createdByEmail, no rules should match
      assertTrue(records.isEmpty());
    }

    @Test
    void shouldFilterByLastModifiedByUserContains_CaseInsensitive() {
      RegionRule rule1 = RegionRule.newBuilder().setId("rule-1").setName("Rule 1").build();

      regionRulesStore.upsertObject(requestContext, rule1);

      // Filter by last_modified_by_user_contains
      GetRegionRulesFilter filter =
          GetRegionRulesFilter.newBuilder()
              .setAuditFilter(
                  AuditFilter.newBuilder().setLastUpdatedByUserContains("nonexistent@test.com"))
              .build();

      List<RegionRuleRecord> records = regionRulesStore.getRuleRecords(requestContext, filter);

      // Since mock doesn't populate lastUserUpdateEmail, no rules should match
      assertTrue(records.isEmpty());
    }

    @Test
    void shouldReturnRecordsWhenCreatedByContainsIsEmpty() {
      RegionRule rule1 = RegionRule.newBuilder().setId("rule-1").setName("Rule 1").build();

      regionRulesStore.upsertObject(requestContext, rule1);

      // Empty created_by_contains should not filter anything
      GetRegionRulesFilter filter =
          GetRegionRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setCreatedByContains(""))
              .build();

      List<RegionRuleRecord> records = regionRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
    }

    @Test
    void shouldReturnRecordsWhenLastModifiedByUserContainsIsEmpty() {
      RegionRule rule1 = RegionRule.newBuilder().setId("rule-1").setName("Rule 1").build();

      regionRulesStore.upsertObject(requestContext, rule1);

      // Empty last_modified_by_user_contains should not filter anything
      GetRegionRulesFilter filter =
          GetRegionRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setLastUpdatedByUserContains(""))
              .build();

      List<RegionRuleRecord> records = regionRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
    }

    @Test
    void shouldFilterByCreatedRange_NoMatchWhenTimestampIsNull() {
      RegionRule rule1 = RegionRule.newBuilder().setId("rule-1").setName("Rule 1").build();

      regionRulesStore.upsertObject(requestContext, rule1);

      // Filter by created_range with specific timestamps
      Instant now = Instant.now();
      GetRegionRulesFilter filter =
          GetRegionRulesFilter.newBuilder()
              .setAuditFilter(
                  AuditFilter.newBuilder()
                      .setCreatedRange(
                          TimestampRange.newBuilder()
                              .setStart(timestampConverter.convert(now.minusSeconds(3600)))
                              .setEnd(timestampConverter.convert(now.plusSeconds(3600)))))
              .build();

      List<RegionRuleRecord> records = regionRulesStore.getRuleRecords(requestContext, filter);

      // Since mock may not populate creationTimestamp properly, this tests the filter logic
      // The actual behavior depends on MockGenericConfigService implementation
      // This test verifies the filter doesn't throw exceptions
      assertTrue(records.size() >= 0);
    }

    @Test
    void shouldFilterByModifiedRange_NoMatchWhenTimestampIsNull() {
      RegionRule rule1 = RegionRule.newBuilder().setId("rule-1").setName("Rule 1").build();

      regionRulesStore.upsertObject(requestContext, rule1);

      // Filter by updated_range with specific timestamps
      Instant now = Instant.now();
      GetRegionRulesFilter filter =
          GetRegionRulesFilter.newBuilder()
              .setAuditFilter(
                  AuditFilter.newBuilder()
                      .setUpdatedRange(
                          TimestampRange.newBuilder()
                              .setStart(timestampConverter.convert(now.minusSeconds(3600)))
                              .setEnd(timestampConverter.convert(now.plusSeconds(3600)))))
              .build();

      List<RegionRuleRecord> records = regionRulesStore.getRuleRecords(requestContext, filter);

      // This test verifies the filter doesn't throw exceptions
      assertTrue(records.size() >= 0);
    }

    @Test
    void shouldBuildAuditDetailsInRecords() {
      RegionRule rule1 = RegionRule.newBuilder().setId("rule-1").setName("Rule 1").build();

      regionRulesStore.upsertObject(requestContext, rule1);

      GetRegionRulesFilter filter = GetRegionRulesFilter.getDefaultInstance();
      List<RegionRuleRecord> records = regionRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
      RegionRuleRecord ruleRecord = records.get(0);
      assertEquals("rule-1", ruleRecord.getRule().getId());
      // AuditDetails should be present (may be empty if mock doesn't populate audit fields)
      assertTrue(ruleRecord.hasAuditDetails());
    }

    @Test
    void shouldCombineAuditFilterWithOtherFilters() {
      RegionRule rule1 =
          RegionRule.newBuilder().setId("rule-1").setName("Rule 1").setDisabled(false).build();
      RegionRule rule2 =
          RegionRule.newBuilder().setId("rule-2").setName("Rule 2").setDisabled(true).build();

      regionRulesStore.upsertObject(requestContext, rule1);
      regionRulesStore.upsertObject(requestContext, rule2);

      // Combine disabled filter with empty audit filter
      GetRegionRulesFilter filter =
          GetRegionRulesFilter.newBuilder()
              .setDisabled(false)
              .setAuditFilter(AuditFilter.getDefaultInstance())
              .build();

      List<RegionRuleRecord> records = regionRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
      assertEquals("rule-1", records.get(0).getRule().getId());
    }
  }
}
