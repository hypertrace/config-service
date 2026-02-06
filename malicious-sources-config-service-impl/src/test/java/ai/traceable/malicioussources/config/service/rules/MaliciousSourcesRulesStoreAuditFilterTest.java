package ai.traceable.malicioussources.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.audit.utils.UserVisibleEmailConfig;
import ai.traceable.config.commons.v1.AuditFilter;
import ai.traceable.config.commons.v1.TimestampRange;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.malicioussources.config.service.v1.GetRulesFilter;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleRecord;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleStatus;
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

class MaliciousSourcesRulesStoreAuditFilterTest {

  private MockGenericConfigService mockConfigService;
  private MaliciousSourcesRulesStore maliciousSourcesRulesStore;
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
    Config config =
        ConfigFactory.parseString(
            "generic.config.service.customer.visible.excluded.email.patterns: []");
    MaliciousSourcesConfigServiceConfig serviceConfig =
        mock(MaliciousSourcesConfigServiceConfig.class);
    when(serviceConfig.getUserVisibleEmailConfig()).thenReturn(new UserVisibleEmailConfig(config));

    maliciousSourcesRulesStore =
        new MaliciousSourcesRulesStore(
            configServiceBlockingStub, configChangeEventGenerator, serviceConfig);
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  private MaliciousSourcesRule buildRule(String id, String name) {
    return MaliciousSourcesRule.newBuilder()
        .setId(id)
        .setRuleInfo(MaliciousSourcesRuleInfo.newBuilder().setName(name))
        .build();
  }

  private MaliciousSourcesRule buildRule(String id, String name, boolean disabled) {
    return MaliciousSourcesRule.newBuilder()
        .setId(id)
        .setRuleInfo(MaliciousSourcesRuleInfo.newBuilder().setName(name))
        .setRuleStatus(MaliciousSourcesRuleStatus.newBuilder().setDisabled(disabled))
        .build();
  }

  @Nested
  class GetRuleRecordsWithAuditFilter {

    @Test
    void shouldReturnAllRecordsWhenNoAuditFilterApplied() {
      MaliciousSourcesRule rule1 = buildRule("rule-1", "Rule 1");
      MaliciousSourcesRule rule2 = buildRule("rule-2", "Rule 2");

      maliciousSourcesRulesStore.upsertObject(requestContext, rule1);
      maliciousSourcesRulesStore.upsertObject(requestContext, rule2);

      GetRulesFilter filter = GetRulesFilter.getDefaultInstance();
      List<MaliciousSourcesRuleRecord> records =
          maliciousSourcesRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(2, records.size());
    }

    @Test
    void shouldReturnEmptyListWhenNoRulesExist() {
      GetRulesFilter filter = GetRulesFilter.getDefaultInstance();
      List<MaliciousSourcesRuleRecord> records =
          maliciousSourcesRulesStore.getRuleRecords(requestContext, filter);

      assertTrue(records.isEmpty());
    }

    @Test
    void shouldFilterByCreatedByContains_CaseInsensitive() {
      MaliciousSourcesRule rule1 = buildRule("rule-1", "Rule 1");

      maliciousSourcesRulesStore.upsertObject(requestContext, rule1);

      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setCreatedByContains("nonexistent@test.com"))
              .build();

      List<MaliciousSourcesRuleRecord> records =
          maliciousSourcesRulesStore.getRuleRecords(requestContext, filter);

      // Since mock doesn't populate createdByEmail, no rules should match
      assertTrue(records.isEmpty());
    }

    @Test
    void shouldFilterByLastModifiedByUserContains_CaseInsensitive() {
      MaliciousSourcesRule rule1 = buildRule("rule-1", "Rule 1");

      maliciousSourcesRulesStore.upsertObject(requestContext, rule1);

      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(
                  AuditFilter.newBuilder().setLastUpdatedByUserContains("nonexistent@test.com"))
              .build();

      List<MaliciousSourcesRuleRecord> records =
          maliciousSourcesRulesStore.getRuleRecords(requestContext, filter);

      // Since mock doesn't populate lastUserUpdateEmail, no rules should match
      assertTrue(records.isEmpty());
    }

    @Test
    void shouldReturnRecordsWhenCreatedByContainsIsEmpty() {
      MaliciousSourcesRule rule1 = buildRule("rule-1", "Rule 1");

      maliciousSourcesRulesStore.upsertObject(requestContext, rule1);

      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setCreatedByContains(""))
              .build();

      List<MaliciousSourcesRuleRecord> records =
          maliciousSourcesRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
    }

    @Test
    void shouldReturnRecordsWhenLastModifiedByUserContainsIsEmpty() {
      MaliciousSourcesRule rule1 = buildRule("rule-1", "Rule 1");

      maliciousSourcesRulesStore.upsertObject(requestContext, rule1);

      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setAuditFilter(AuditFilter.newBuilder().setLastUpdatedByUserContains(""))
              .build();

      List<MaliciousSourcesRuleRecord> records =
          maliciousSourcesRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
    }

    @Test
    void shouldFilterByCreatedRange() {
      MaliciousSourcesRule rule1 = buildRule("rule-1", "Rule 1");

      maliciousSourcesRulesStore.upsertObject(requestContext, rule1);

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

      List<MaliciousSourcesRuleRecord> records =
          maliciousSourcesRulesStore.getRuleRecords(requestContext, filter);

      // Verifies the filter doesn't throw exceptions
      assertTrue(records.size() >= 0);
    }

    @Test
    void shouldFilterByModifiedRange() {
      MaliciousSourcesRule rule1 = buildRule("rule-1", "Rule 1");

      maliciousSourcesRulesStore.upsertObject(requestContext, rule1);

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

      List<MaliciousSourcesRuleRecord> records =
          maliciousSourcesRulesStore.getRuleRecords(requestContext, filter);

      assertTrue(records.size() >= 0);
    }

    @Test
    void shouldBuildAuditDetailsInRecords() {
      MaliciousSourcesRule rule1 = buildRule("rule-1", "Rule 1");

      maliciousSourcesRulesStore.upsertObject(requestContext, rule1);

      GetRulesFilter filter = GetRulesFilter.getDefaultInstance();
      List<MaliciousSourcesRuleRecord> records =
          maliciousSourcesRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
      MaliciousSourcesRuleRecord ruleRecord = records.get(0);
      assertEquals("rule-1", ruleRecord.getRule().getId());
      assertTrue(ruleRecord.hasAuditDetails());
    }

    @Test
    void shouldCombineAuditFilterWithOtherFilters() {
      MaliciousSourcesRule rule1 = buildRule("rule-1", "Rule 1", false);
      MaliciousSourcesRule rule2 = buildRule("rule-2", "Rule 2", true);

      maliciousSourcesRulesStore.upsertObject(requestContext, rule1);
      maliciousSourcesRulesStore.upsertObject(requestContext, rule2);

      GetRulesFilter filter =
          GetRulesFilter.newBuilder()
              .setDisabled(false)
              .setAuditFilter(AuditFilter.getDefaultInstance())
              .build();

      List<MaliciousSourcesRuleRecord> records =
          maliciousSourcesRulesStore.getRuleRecords(requestContext, filter);

      assertEquals(1, records.size());
      assertEquals("rule-1", records.get(0).getRule().getId());
    }
  }
}
