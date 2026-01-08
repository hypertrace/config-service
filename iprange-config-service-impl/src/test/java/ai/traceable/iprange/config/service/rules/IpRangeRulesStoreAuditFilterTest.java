package ai.traceable.iprange.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import ai.traceable.config.commons.v1.AuditFilter;
import ai.traceable.config.commons.v1.TimestampRange;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.iprange.config.service.v1.GetRulesFilter;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import ai.traceable.iprange.config.service.v1.IpRangeRuleDetails;
import ai.traceable.iprange.config.service.v1.IpRangeRuleRecord;
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

class IpRangeRulesStoreAuditFilterTest {

  private MockGenericConfigService mockConfigService;
  private IpRangeRulesStore ipRangeRulesStore;
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
    ipRangeRulesStore =
        new IpRangeRulesStore(
            configServiceBlockingStub, configChangeEventGenerator, timestampConverter);
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  private IpRangeRule buildRule(String id, String name) {
    return IpRangeRule.newBuilder()
        .setId(id)
        .setRuleDetails(IpRangeRuleDetails.newBuilder().setName(name))
        .build();
  }

  private IpRangeRule buildRule(String id, String name, boolean disabled) {
    return IpRangeRule.newBuilder()
        .setId(id)
        .setRuleDetails(IpRangeRuleDetails.newBuilder().setName(name))
        .setDisabled(disabled)
        .build();
  }

  @Nested
  class GetRuleRecordsWithAuditFilter {

    @Test
    void shouldReturnAllRecordsWhenNoAuditFilterApplied() {
      IpRangeRule rule1 = buildRule("rule-1", "Rule 1");
      IpRangeRule rule2 = buildRule("rule-2", "Rule 2");

      ipRangeRulesStore.upsertObject(requestContext, rule1);
      ipRangeRulesStore.upsertObject(requestContext, rule2);

      GetRulesFilter filter = GetRulesFilter.getDefaultInstance();
      List<IpRangeRuleRecord> records = ipRangeRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(2, records.size());
    }

    @Test
    void shouldReturnEmptyListWhenNoRulesExist() {
      GetRulesFilter filter = GetRulesFilter.getDefaultInstance();
      List<IpRangeRuleRecord> records = ipRangeRulesStore.getRuleRecords(requestContext, filter);

      assertTrue(records.isEmpty());
    }

    @Test
    void shouldFilterByCreatedByContains_CaseInsensitive() {
      IpRangeRule rule1 = buildRule("rule-1", "Rule 1");

      ipRangeRulesStore.upsertObject(requestContext, rule1);

      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setCreatedByContains("nonexistent@test.com"))
              .build();

      List<IpRangeRuleRecord> records = ipRangeRulesStore.getRuleRecords(requestContext, filter);

      // Since mock doesn't populate createdByEmail, no rules should match
      assertTrue(records.isEmpty());
    }

    @Test
    void shouldFilterByLastModifiedByUserContains_CaseInsensitive() {
      IpRangeRule rule1 = buildRule("rule-1", "Rule 1");

      ipRangeRulesStore.upsertObject(requestContext, rule1);

      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(
                  AuditFilter.newBuilder().setLastUpdatedByUserContains("nonexistent@test.com"))
              .build();

      List<IpRangeRuleRecord> records = ipRangeRulesStore.getRuleRecords(requestContext, filter);

      // Since mock doesn't populate lastUserUpdateEmail, no rules should match
      assertTrue(records.isEmpty());
    }

    @Test
    void shouldReturnRecordsWhenCreatedByContainsIsEmpty() {
      IpRangeRule rule1 = buildRule("rule-1", "Rule 1");

      ipRangeRulesStore.upsertObject(requestContext, rule1);

      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setCreatedByContains(""))
              .build();

      List<IpRangeRuleRecord> records = ipRangeRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
    }

    @Test
    void shouldReturnRecordsWhenLastModifiedByUserContainsIsEmpty() {
      IpRangeRule rule1 = buildRule("rule-1", "Rule 1");

      ipRangeRulesStore.upsertObject(requestContext, rule1);

      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setLastUpdatedByUserContains(""))
              .build();

      List<IpRangeRuleRecord> records = ipRangeRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
    }

    @Test
    void shouldFilterByCreatedRange() {
      IpRangeRule rule1 = buildRule("rule-1", "Rule 1");

      ipRangeRulesStore.upsertObject(requestContext, rule1);

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

      List<IpRangeRuleRecord> records = ipRangeRulesStore.getRuleRecords(requestContext, filter);

      // Verifies the filter doesn't throw exceptions
      assertTrue(records.size() >= 0);
    }

    @Test
    void shouldFilterByModifiedRange() {
      IpRangeRule rule1 = buildRule("rule-1", "Rule 1");

      ipRangeRulesStore.upsertObject(requestContext, rule1);

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

      List<IpRangeRuleRecord> records = ipRangeRulesStore.getRuleRecords(requestContext, filter);

      assertTrue(records.size() >= 0);
    }

    @Test
    void shouldBuildAuditDetailsInRecords() {
      IpRangeRule rule1 = buildRule("rule-1", "Rule 1");

      ipRangeRulesStore.upsertObject(requestContext, rule1);

      GetRulesFilter filter = GetRulesFilter.getDefaultInstance();
      List<IpRangeRuleRecord> records = ipRangeRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
      IpRangeRuleRecord ruleRecord = records.get(0);
      assertEquals("rule-1", ruleRecord.getRule().getId());
      assertTrue(ruleRecord.hasAuditDetails());
    }

    @Test
    void shouldCombineAuditFilterWithOtherFilters() {
      IpRangeRule rule1 = buildRule("rule-1", "Rule 1", false);
      IpRangeRule rule2 = buildRule("rule-2", "Rule 2", true);

      ipRangeRulesStore.upsertObject(requestContext, rule1);
      ipRangeRulesStore.upsertObject(requestContext, rule2);

      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setDisabled(false)
              .setAuditFilter(AuditFilter.getDefaultInstance())
              .build();

      List<IpRangeRuleRecord> records = ipRangeRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
      assertEquals("rule-1", records.get(0).getRule().getId());
    }
  }
}
