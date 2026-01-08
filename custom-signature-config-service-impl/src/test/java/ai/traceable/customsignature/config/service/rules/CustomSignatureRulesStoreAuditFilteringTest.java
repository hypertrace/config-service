package ai.traceable.customsignature.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.commons.v1.AuditFilter;
import ai.traceable.config.commons.v1.TimestampRange;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.customsignature.config.service.CustomSignatureConfigServiceConfig;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRuleRecord;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ContextualConfigObject;
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
                contextualRule(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                contextualRule(
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
                contextualRule(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                contextualRule(
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
                contextualRule(
                    "id-1",
                    Instant.ofEpochSecond(99),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                contextualRule(
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
    CustomSignatureRule ruleWithNoCreation = CustomSignatureRule.newBuilder().setId("id-1").build();
    ContextualConfigObject<CustomSignatureRule> noCreationTimestamp =
        new ContextualConfigObjectTestImpl<>(
            ruleWithNoCreation,
            "id-1",
            null,
            "alice@example.com",
            Instant.ofEpochSecond(200),
            "bob@example.com",
            Instant.ofEpochSecond(200),
            "bob@example.com");

    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                noCreationTimestamp,
                contextualRule(
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
                contextualRule(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                contextualRule(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "charlie@example.com"),
                contextualRule(
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
  void testModifiedRangeExcludesNullLastUpdateTimestamp() {
    CustomSignatureRule ruleWithNoModification =
        CustomSignatureRule.newBuilder().setId("id-1").build();
    ContextualConfigObject<CustomSignatureRule> noModificationTimestamp =
        new ContextualConfigObjectTestImpl<>(
            ruleWithNoModification,
            "id-1",
            Instant.ofEpochSecond(100),
            "alice@example.com",
            null,
            "bob@example.com",
            null,
            "bob@example.com");

    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                noModificationTimestamp,
                contextualRule(
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
  void testModifiedRangeExcludesZeroEpochLastUpdateTimestamp() {
    CustomSignatureRule ruleWithZeroModification =
        CustomSignatureRule.newBuilder().setId("id-1").build();
    ContextualConfigObject<CustomSignatureRule> zeroModificationTimestamp =
        new ContextualConfigObjectTestImpl<>(
            ruleWithZeroModification,
            "id-1",
            Instant.ofEpochSecond(100),
            "alice@example.com",
            Instant.ofEpochSecond(0),
            "bob@example.com",
            Instant.ofEpochSecond(0),
            "bob@example.com");

    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                zeroModificationTimestamp,
                contextualRule(
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
  void testCreatedByContainsExcludesNullCreatedBy() {
    CustomSignatureRule ruleWithNullCreatedBy =
        CustomSignatureRule.newBuilder().setId("id-1").build();
    ContextualConfigObject<CustomSignatureRule> nullCreatedBy =
        new ContextualConfigObjectTestImpl<>(
            ruleWithNullCreatedBy,
            "id-1",
            Instant.ofEpochSecond(100),
            null,
            Instant.ofEpochSecond(200),
            "bob@example.com",
            Instant.ofEpochSecond(200),
            "bob@example.com");

    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                nullCreatedBy,
                contextualRule(
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
  void testCreatedByContainsExcludesEmptyCreatedBy() {
    CustomSignatureRule ruleWithEmptyCreatedBy =
        CustomSignatureRule.newBuilder().setId("id-1").build();
    ContextualConfigObject<CustomSignatureRule> emptyCreatedBy =
        new ContextualConfigObjectTestImpl<>(
            ruleWithEmptyCreatedBy,
            "id-1",
            Instant.ofEpochSecond(100),
            "",
            Instant.ofEpochSecond(200),
            "bob@example.com",
            Instant.ofEpochSecond(200),
            "bob@example.com");

    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                emptyCreatedBy,
                contextualRule(
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
  void testLastModifiedByContainsExcludesNullLastModifiedBy() {
    CustomSignatureRule ruleWithNullLastModifiedBy =
        CustomSignatureRule.newBuilder().setId("id-1").build();
    ContextualConfigObject<CustomSignatureRule> nullLastModifiedBy =
        new ContextualConfigObjectTestImpl<>(
            ruleWithNullLastModifiedBy,
            "id-1",
            Instant.ofEpochSecond(100),
            "alice@example.com",
            Instant.ofEpochSecond(200),
            null,
            Instant.ofEpochSecond(200),
            null);

    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                nullLastModifiedBy,
                contextualRule(
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
  void testLastModifiedByContainsExcludesEmptyLastModifiedBy() {
    CustomSignatureRule ruleWithEmptyLastModifiedBy =
        CustomSignatureRule.newBuilder().setId("id-1").build();
    ContextualConfigObject<CustomSignatureRule> emptyLastModifiedBy =
        new ContextualConfigObjectTestImpl<>(
            ruleWithEmptyLastModifiedBy,
            "id-1",
            Instant.ofEpochSecond(100),
            "alice@example.com",
            Instant.ofEpochSecond(200),
            "",
            Instant.ofEpochSecond(200),
            "");

    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                emptyLastModifiedBy,
                contextualRule(
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
                contextualRule(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                contextualRule(
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
                contextualRule(
                    "id-1",
                    Instant.ofEpochSecond(300),
                    "alice@example.com",
                    Instant.ofEpochSecond(400),
                    "bob@example.com"),
                contextualRule(
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
                contextualRule(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(400),
                    "bob@example.com"),
                contextualRule(
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
                contextualRule(
                    "id-1",
                    Instant.ofEpochSecond(50),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                contextualRule(
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
  void testCreatedRangeExcludesNullCreationTimestamp() {
    CustomSignatureRule ruleWithNullCreation =
        CustomSignatureRule.newBuilder().setId("id-1").build();
    ContextualConfigObject<CustomSignatureRule> nullCreationTimestamp =
        new ContextualConfigObjectTestImpl<>(
            ruleWithNullCreation,
            "id-1",
            null,
            "alice@example.com",
            Instant.ofEpochSecond(200),
            "bob@example.com",
            Instant.ofEpochSecond(200),
            "bob@example.com");

    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                nullCreationTimestamp,
                contextualRule(
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
                contextualRule(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                contextualRule(
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
  void testGetAllRuleRecordsBuildsAuditDetails() {
    CustomSignatureRulesStore store =
        newTestStore(
            List.of(
                contextualRule(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                contextualRule("id-2", Instant.EPOCH, "", Instant.EPOCH, "")));

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

  private static ContextualConfigObject<CustomSignatureRule> contextualRule(
      String id,
      Instant creationTimestamp,
      String createdByEmail,
      Instant lastUserUpdateTimestamp,
      String lastUserUpdateEmail) {
    CustomSignatureRule rule = CustomSignatureRule.newBuilder().setId(id).build();
    return new ContextualConfigObjectTestImpl<>(
        rule,
        id,
        creationTimestamp,
        createdByEmail,
        lastUserUpdateTimestamp,
        lastUserUpdateEmail,
        lastUserUpdateTimestamp,
        lastUserUpdateEmail);
  }

  private static CustomSignatureRulesStore newTestStore(
      List<ContextualConfigObject<CustomSignatureRule>> objects) {
    CustomSignatureConfigServiceConfig config = mock(CustomSignatureConfigServiceConfig.class);
    when(config.getDefaultCustomSignatureRules()).thenReturn(List.of());

    return new TestCustomSignatureRulesStore(objects, config, new TimestampConverter());
  }

  private static class TestCustomSignatureRulesStore extends CustomSignatureRulesStore {

    private final List<ContextualConfigObject<CustomSignatureRule>> objects;

    TestCustomSignatureRulesStore(
        List<ContextualConfigObject<CustomSignatureRule>> objects,
        CustomSignatureConfigServiceConfig config,
        TimestampConverter timestampConverter) {
      super(
          mock(org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub.class),
          new CustomSignatureRuleConverter(),
          mock(org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator.class),
          config,
          timestampConverter);
      this.objects = objects;
    }

    @Override
    public List<ContextualConfigObject<CustomSignatureRule>> getAllObjects(RequestContext context) {
      return objects;
    }

    @Override
    public List<ContextualConfigObject<CustomSignatureRule>> getAllObjects(
        RequestContext context, GetRulesFilter filter) {
      return objects.stream()
          .filter(obj -> filterConfigData(obj.getData(), filter).isPresent())
          .filter(obj -> invokeMatchesAuditFilters(obj, filter))
          .collect(java.util.stream.Collectors.toList());
    }

    private boolean invokeMatchesAuditFilters(
        ContextualConfigObject<CustomSignatureRule> configObject, GetRulesFilter filter) {
      try {
        java.lang.reflect.Method method =
            CustomSignatureRulesStore.class.getDeclaredMethod(
                "matchesAuditFilters", ContextualConfigObject.class, GetRulesFilter.class);
        method.setAccessible(true);
        return (boolean) method.invoke(this, configObject, filter);
      } catch (Exception e) {
        throw new RuntimeException("Failed to invoke matchesAuditFilters via reflection", e);
      }
    }
  }

  private static class ContextualConfigObjectTestImpl<T> implements ContextualConfigObject<T> {
    private final T data;
    private final String context;
    private final Instant creationTimestamp;
    private final String createdByEmail;
    private final Instant lastUserUpdateTimestamp;
    private final String lastUserUpdateEmail;
    private final Instant lastUpdatedTimestamp;
    private final String lastUpdateEmail;

    ContextualConfigObjectTestImpl(
        T data,
        String context,
        Instant creationTimestamp,
        String createdByEmail,
        Instant lastUserUpdateTimestamp,
        String lastUserUpdateEmail,
        Instant lastUpdatedTimestamp,
        String lastUpdateEmail) {
      this.data = data;
      this.context = context;
      this.creationTimestamp = creationTimestamp;
      this.createdByEmail = createdByEmail;
      this.lastUserUpdateTimestamp = lastUserUpdateTimestamp;
      this.lastUserUpdateEmail = lastUserUpdateEmail;
      this.lastUpdatedTimestamp = lastUpdatedTimestamp;
      this.lastUpdateEmail = lastUpdateEmail;
    }

    @Override
    public T getData() {
      return data;
    }

    @Override
    public String getContext() {
      return context;
    }

    @Override
    public Instant getCreationTimestamp() {
      return creationTimestamp;
    }

    @Override
    public String getCreatedByEmail() {
      return createdByEmail;
    }

    @Override
    public Instant getLastUserUpdateTimestamp() {
      return lastUserUpdateTimestamp;
    }

    @Override
    public String getLastUserUpdateEmail() {
      return lastUserUpdateEmail;
    }

    @Override
    public Instant getLastUpdatedTimestamp() {
      return lastUpdatedTimestamp;
    }

    @Override
    public String getLastUpdateEmail() {
      return lastUpdateEmail;
    }
  }
}
