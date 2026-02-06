package ai.traceable.customsignature.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.audit.utils.UserVisibleEmailConfig;
import ai.traceable.config.commons.v1.AuditFilter;
import ai.traceable.config.commons.v1.TimestampRange;
import ai.traceable.customsignature.config.service.CustomSignatureConfigServiceConfig;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRuleRecord;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import com.google.protobuf.Timestamp;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.GetAllConfigsResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class CustomSignatureRulesStoreAuditFilteringTest {

  private static final RequestContext REQUEST_CONTEXT =
      RequestContext.forTenantId("audit-filtering-test-tenant");

  @Test
  void testCreatedByContainsMatchesCaseInsensitive() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                buildContextSpecificConfig(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                buildContextSpecificConfig(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .setAuditFilter(AuditFilter.newBuilder().setCreatedByContains("ALICE"))
            .build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-1", results.get(0).getRule().getId());
  }

  @Test
  void testCreatedByContainsEmptyDoesNotFilter() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                buildContextSpecificConfig(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                buildContextSpecificConfig(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .setAuditFilter(AuditFilter.newBuilder().setCreatedByContains(""))
            .build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(2, results.size());
  }

  @Test
  void testCreatedRangeInclusiveBounds() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                buildContextSpecificConfig(
                    "id-1",
                    Instant.ofEpochSecond(99),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                buildContextSpecificConfig(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    TimestampRange range =
        TimestampRange.newBuilder()
            .setStart(Timestamp.newBuilder().setSeconds(100).build())
            .setEnd(Timestamp.newBuilder().setSeconds(100).build())
            .build();

    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .setAuditFilter(AuditFilter.newBuilder().setCreatedRange(range))
            .build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testCreatedRangeWithoutBoundsStillRequiresCreationTimestampPresent() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                buildContextSpecificConfig(
                    "id-1",
                    Instant.EPOCH,
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                buildContextSpecificConfig(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    TimestampRange range = TimestampRange.newBuilder().build();
    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .setAuditFilter(AuditFilter.newBuilder().setCreatedRange(range))
            .build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testModifiedRangeAndLastModifiedByContainsAreAppliedWithAndSemantics() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                buildContextSpecificConfig(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                buildContextSpecificConfig(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "charlie@example.com"),
                buildContextSpecificConfig(
                    "id-3",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(199),
                    "bob@example.com")));

    TimestampRange range =
        TimestampRange.newBuilder()
            .setStart(Timestamp.newBuilder().setSeconds(200).build())
            .build();

    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .setAuditFilter(
                AuditFilter.newBuilder().setUpdatedRange(range).setLastUpdatedByUserContains("BOB"))
            .build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-1", results.get(0).getRule().getId());
  }

  @Test
  void testModifiedRangeExcludesZeroEpochLastUpdateTimestamp() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                buildContextSpecificConfig(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.EPOCH,
                    "bob@example.com"),
                buildContextSpecificConfig(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    TimestampRange range =
        TimestampRange.newBuilder()
            .setStart(Timestamp.newBuilder().setSeconds(150).build())
            .build();
    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .setAuditFilter(AuditFilter.newBuilder().setUpdatedRange(range))
            .build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testModifiedRangeWithEmptyRangeExcludesZeroEpochLastUpdateTimestamp() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                buildContextSpecificConfig(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.EPOCH,
                    "bob@example.com"),
                buildContextSpecificConfig(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    TimestampRange range = TimestampRange.newBuilder().build();
    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .setAuditFilter(AuditFilter.newBuilder().setUpdatedRange(range))
            .build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testCreatedByContainsExcludesEmptyCreatedBy() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                buildContextSpecificConfig(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                buildContextSpecificConfig(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .setAuditFilter(AuditFilter.newBuilder().setCreatedByContains("charlie"))
            .build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testLastModifiedByContainsExcludesEmptyLastModifiedBy() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                buildContextSpecificConfig(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    ""),
                buildContextSpecificConfig(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .setAuditFilter(AuditFilter.newBuilder().setLastUpdatedByUserContains("dave"))
            .build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testLastModifiedByContainsEmptyDoesNotFilter() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                buildContextSpecificConfig(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                buildContextSpecificConfig(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .setAuditFilter(AuditFilter.newBuilder().setLastUpdatedByUserContains(""))
            .build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(2, results.size());
  }

  @Test
  void testCreatedRangeWithEndOnlyExcludesTimestampAfterEnd() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                buildContextSpecificConfig(
                    "id-1",
                    Instant.ofEpochSecond(300),
                    "alice@example.com",
                    Instant.ofEpochSecond(400),
                    "bob@example.com"),
                buildContextSpecificConfig(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    TimestampRange range =
        TimestampRange.newBuilder().setEnd(Timestamp.newBuilder().setSeconds(200).build()).build();

    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .setAuditFilter(AuditFilter.newBuilder().setCreatedRange(range))
            .build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testModifiedRangeWithEndOnlyExcludesTimestampAfterEnd() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                buildContextSpecificConfig(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(400),
                    "bob@example.com"),
                buildContextSpecificConfig(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    TimestampRange range =
        TimestampRange.newBuilder().setEnd(Timestamp.newBuilder().setSeconds(200).build()).build();

    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .setAuditFilter(AuditFilter.newBuilder().setUpdatedRange(range))
            .build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testCreatedRangeExcludesTimestampBeforeStart() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                buildContextSpecificConfig(
                    "id-1",
                    Instant.ofEpochSecond(50),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                buildContextSpecificConfig(
                    "id-2",
                    Instant.ofEpochSecond(150),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    TimestampRange range =
        TimestampRange.newBuilder()
            .setStart(Timestamp.newBuilder().setSeconds(100).build())
            .build();

    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .setAuditFilter(AuditFilter.newBuilder().setCreatedRange(range))
            .build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testCreatedRangeExcludesZeroCreationTimestamp() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                buildContextSpecificConfig(
                    "id-1",
                    Instant.EPOCH,
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                buildContextSpecificConfig(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    TimestampRange range =
        TimestampRange.newBuilder().setStart(Timestamp.newBuilder().setSeconds(50).build()).build();
    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .setAuditFilter(AuditFilter.newBuilder().setCreatedRange(range))
            .build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testNoAuditFilterReturnsAllRecords() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                buildContextSpecificConfig(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                buildContextSpecificConfig(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    GetRulesFilter filter = GetRulesFilter.newBuilder().build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(2, results.size());
  }

  @Test
  void testAuditFilterExcludesDefaultRules() {
    // Setup: store with one stored rule and one default rule
    CustomSignatureRule defaultRule = CustomSignatureRule.newBuilder().setId("default-1").build();

    CustomSignatureRulesStore store =
        newTestStoreWithDefaults(
            List.of(
                buildContextSpecificConfig(
                    "stored-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com")),
            List.of(defaultRule));

    // Apply audit filter (created_by_contains) - should exclude default rules
    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .setAuditFilter(AuditFilter.newBuilder().setCreatedByContains("alice"))
            .build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);

    // Assert: only stored rule is returned, default rule is excluded
    assertEquals(1, results.size());
    assertEquals("stored-1", results.get(0).getRule().getId());
  }

  @Test
  void testEmptyAuditFilterStillIncludesDefaultRules() {
    // Setup: store with one stored rule and one default rule
    CustomSignatureRule defaultRule = CustomSignatureRule.newBuilder().setId("default-1").build();

    CustomSignatureRulesStore store =
        newTestStoreWithDefaults(
            List.of(
                buildContextSpecificConfig(
                    "stored-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com")),
            List.of(defaultRule));

    // Apply empty audit filter - should still include default rules
    GetRulesFilter filter =
        GetRulesFilter.newBuilder().setAuditFilter(AuditFilter.newBuilder().build()).build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);

    // Assert: both stored and default rules are returned
    assertEquals(2, results.size());
  }

  @Test
  void testNoAuditFilterIncludesDefaultRules() {
    // Setup: store with one stored rule and one default rule
    CustomSignatureRule defaultRule = CustomSignatureRule.newBuilder().setId("default-1").build();

    CustomSignatureRulesStore store =
        newTestStoreWithDefaults(
            List.of(
                buildContextSpecificConfig(
                    "stored-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com")),
            List.of(defaultRule));

    // No audit filter - should include default rules
    GetRulesFilter filter = GetRulesFilter.newBuilder().build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);

    // Assert: both stored and default rules are returned
    assertEquals(2, results.size());
  }

  @Test
  void testCreatedRangeAuditFilterExcludesDefaultRules() {
    // Setup: store with one stored rule and one default rule
    CustomSignatureRule defaultRule = CustomSignatureRule.newBuilder().setId("default-1").build();

    CustomSignatureRulesStore store =
        newTestStoreWithDefaults(
            List.of(
                buildContextSpecificConfig(
                    "stored-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com")),
            List.of(defaultRule));

    // Apply created_range audit filter - should exclude default rules
    TimestampRange range =
        TimestampRange.newBuilder().setStart(Timestamp.newBuilder().setSeconds(50).build()).build();
    GetRulesFilter filter =
        GetRulesFilter.newBuilder()
            .setAuditFilter(AuditFilter.newBuilder().setCreatedRange(range))
            .build();

    List<CustomSignatureRuleRecord> results = store.getAllRuleRecords(REQUEST_CONTEXT, filter);

    // Assert: only stored rule is returned, default rule is excluded
    assertEquals(1, results.size());
    assertEquals("stored-1", results.get(0).getRule().getId());
  }

  @Test
  void testGetAllRuleRecordsBuildsAuditDetails() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                buildContextSpecificConfig(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                buildContextSpecificConfig("id-2", Instant.EPOCH, "", Instant.EPOCH, "")));

    List<CustomSignatureRuleRecord> records = store.getAllRuleRecords(REQUEST_CONTEXT);
    Map<String, CustomSignatureRuleRecord> recordsById =
        records.stream().collect(Collectors.toMap(r -> r.getRule().getId(), Function.identity()));

    assertTrue(recordsById.containsKey("id-1"));
    CustomSignatureRuleRecord record1 = recordsById.get("id-1");
    assertTrue(record1.hasAuditDetails());
    assertTrue(record1.getAuditDetails().hasCreationDetails());
    assertEquals(
        "alice@example.com", record1.getAuditDetails().getCreationDetails().getCreatedBy());
    assertEquals(
        Timestamp.newBuilder().setSeconds(100).build(),
        record1.getAuditDetails().getCreationDetails().getCreatedAt());
    assertTrue(record1.getAuditDetails().hasLastUserUpdateDetails());
    assertEquals(
        "bob@example.com", record1.getAuditDetails().getLastUserUpdateDetails().getUpdatedBy());
    assertEquals(
        Timestamp.newBuilder().setSeconds(200).build(),
        record1.getAuditDetails().getLastUserUpdateDetails().getUpdatedAt());

    assertTrue(recordsById.containsKey("id-2"));
    CustomSignatureRuleRecord record2 = recordsById.get("id-2");
    assertTrue(record2.hasAuditDetails());
    assertFalse(record2.getAuditDetails().hasCreationDetails());
    assertFalse(record2.getAuditDetails().hasLastUserUpdateDetails());
  }

  private static ContextSpecificConfig buildContextSpecificConfig(
      String id,
      Instant creationTimestamp,
      String createdByEmail,
      Instant lastUserUpdateTimestamp,
      String lastUserUpdateEmail) {
    try {
      CustomSignatureRule rule = CustomSignatureRule.newBuilder().setId(id).build();
      return ContextSpecificConfig.newBuilder()
          .setContext(id)
          .setConfig(ConfigProtoConverter.convertToValue(rule))
          .setCreationTimestamp(creationTimestamp.toEpochMilli())
          .setCreatedByEmail(createdByEmail)
          .setLastUserUpdateTimestamp(lastUserUpdateTimestamp.toEpochMilli())
          .setLastUserUpdateEmail(lastUserUpdateEmail)
          .setUpdateTimestamp(lastUserUpdateTimestamp.toEpochMilli())
          .setLastUpdateEmail(lastUserUpdateEmail)
          .build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static CustomSignatureRulesStore newTestStore(List<ContextSpecificConfig> configs) {
    Config typesafeConfig =
        ConfigFactory.parseString(
            "generic.config.service.customer.visible.excluded.email.patterns: []");
    CustomSignatureConfigServiceConfig config = mock(CustomSignatureConfigServiceConfig.class);
    when(config.getDefaultCustomSignatureRules()).thenReturn(List.of());
    when(config.getUserVisibleEmailConfig()).thenReturn(new UserVisibleEmailConfig(typesafeConfig));

    ConfigServiceBlockingStub stub = mock(ConfigServiceBlockingStub.class);
    when(stub.withDeadline(any())).thenReturn(stub);
    when(stub.getAllConfigs(any()))
        .thenReturn(
            GetAllConfigsResponse.newBuilder().addAllContextSpecificConfigs(configs).build());

    return new CustomSignatureRulesStore(
        stub, new CustomSignatureRuleConverter(), mock(ConfigChangeEventGenerator.class), config);
  }

  private static CustomSignatureRulesStore newTestStoreWithDefaults(
      List<ContextSpecificConfig> configs, List<CustomSignatureRule> defaultRules) {
    Config typesafeConfig =
        ConfigFactory.parseString(
            "generic.config.service.customer.visible.excluded.email.patterns: []");
    CustomSignatureConfigServiceConfig config = mock(CustomSignatureConfigServiceConfig.class);
    when(config.getDefaultCustomSignatureRules()).thenReturn(defaultRules);
    when(config.getUserVisibleEmailConfig()).thenReturn(new UserVisibleEmailConfig(typesafeConfig));

    ConfigServiceBlockingStub stub = mock(ConfigServiceBlockingStub.class);
    when(stub.withDeadline(any())).thenReturn(stub);
    when(stub.getAllConfigs(any()))
        .thenReturn(
            GetAllConfigsResponse.newBuilder().addAllContextSpecificConfigs(configs).build());

    return new CustomSignatureRulesStore(
        stub, new CustomSignatureRuleConverter(), mock(ConfigChangeEventGenerator.class), config);
  }
}
