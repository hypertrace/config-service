package ai.traceable.edge.decision.config.service.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import ai.traceable.config.commons.v1.AuditFilter;
import ai.traceable.config.commons.v1.TimestampRange;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleRecord;
import ai.traceable.edge.decision.config.service.v1.Filter;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class EdgeDecisionRuleStoreAuditFilteringTest {

  private static final RequestContext REQUEST_CONTEXT =
      RequestContext.forTenantId("audit-filtering-test-tenant");

  @Test
  void testCreatedByContainsMatchesCaseInsensitive() {
    EdgeDecisionRuleStore store =
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

    AuditFilter auditFilter = AuditFilter.newBuilder().setCreatedByContains("ALICE").build();
    Filter filter = Filter.newBuilder().setAuditFilter(auditFilter).build();

    List<EdgeDecisionRuleRecord> results = store.getRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-1", results.get(0).getRule().getId());
  }

  @Test
  void testCreatedByContainsEmptyDoesNotFilter() {
    EdgeDecisionRuleStore store =
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

    AuditFilter auditFilter = AuditFilter.newBuilder().setCreatedByContains("").build();
    Filter filter = Filter.newBuilder().setAuditFilter(auditFilter).build();

    List<EdgeDecisionRuleRecord> results = store.getRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(2, results.size());
  }

  @Test
  void testCreatedRangeInclusiveBounds() {
    EdgeDecisionRuleStore store =
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

    AuditFilter auditFilter = AuditFilter.newBuilder().setCreatedRange(range).build();
    Filter filter = Filter.newBuilder().setAuditFilter(auditFilter).build();

    List<EdgeDecisionRuleRecord> results = store.getRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testCreatedRangeWithoutBoundsStillRequiresCreationTimestampPresent() {
    EdgeDecisionRule ruleWithNoCreation = EdgeDecisionRule.newBuilder().setId("id-1").build();
    ContextualConfigObject<EdgeDecisionRule> noCreationTimestamp =
        new ContextualConfigObjectTestImpl<>(
            ruleWithNoCreation,
            "id-1",
            null,
            "alice@example.com",
            Instant.ofEpochSecond(200),
            "bob@example.com",
            Instant.ofEpochSecond(200),
            "bob@example.com");

    EdgeDecisionRuleStore store =
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
    AuditFilter auditFilter = AuditFilter.newBuilder().setCreatedRange(range).build();
    Filter filter = Filter.newBuilder().setAuditFilter(auditFilter).build();

    List<EdgeDecisionRuleRecord> results = store.getRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testModifiedRangeAndLastModifiedByContainsAreAppliedWithAndSemantics() {
    EdgeDecisionRuleStore store =
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

    AuditFilter auditFilter =
        AuditFilter.newBuilder().setUpdatedRange(range).setLastUpdatedByUserContains("BOB").build();
    Filter filter = Filter.newBuilder().setAuditFilter(auditFilter).build();

    List<EdgeDecisionRuleRecord> results = store.getRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-1", results.get(0).getRule().getId());
  }

  @Test
  void testModifiedRangeExcludesNullLastUpdateTimestamp() {
    EdgeDecisionRule ruleWithNoModification = EdgeDecisionRule.newBuilder().setId("id-1").build();
    ContextualConfigObject<EdgeDecisionRule> noModificationTimestamp =
        new ContextualConfigObjectTestImpl<>(
            ruleWithNoModification,
            "id-1",
            Instant.ofEpochSecond(100),
            "alice@example.com",
            null,
            "bob@example.com",
            null,
            "bob@example.com");

    EdgeDecisionRuleStore store =
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
    AuditFilter auditFilter = AuditFilter.newBuilder().setUpdatedRange(range).build();
    Filter filter = Filter.newBuilder().setAuditFilter(auditFilter).build();

    List<EdgeDecisionRuleRecord> results = store.getRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testCreatedByContainsExcludesNullCreatedBy() {
    EdgeDecisionRule ruleWithNullCreatedBy = EdgeDecisionRule.newBuilder().setId("id-1").build();
    ContextualConfigObject<EdgeDecisionRule> nullCreatedBy =
        new ContextualConfigObjectTestImpl<>(
            ruleWithNullCreatedBy,
            "id-1",
            Instant.ofEpochSecond(100),
            null,
            Instant.ofEpochSecond(200),
            "bob@example.com",
            Instant.ofEpochSecond(200),
            "bob@example.com");

    EdgeDecisionRuleStore store =
        newTestStore(
            List.of(
                nullCreatedBy,
                contextualRule(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    AuditFilter auditFilter = AuditFilter.newBuilder().setCreatedByContains("charlie").build();
    Filter filter = Filter.newBuilder().setAuditFilter(auditFilter).build();

    List<EdgeDecisionRuleRecord> results = store.getRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testCreatedByContainsExcludesEmptyCreatedBy() {
    EdgeDecisionRule ruleWithEmptyCreatedBy = EdgeDecisionRule.newBuilder().setId("id-1").build();
    ContextualConfigObject<EdgeDecisionRule> emptyCreatedBy =
        new ContextualConfigObjectTestImpl<>(
            ruleWithEmptyCreatedBy,
            "id-1",
            Instant.ofEpochSecond(100),
            "",
            Instant.ofEpochSecond(200),
            "bob@example.com",
            Instant.ofEpochSecond(200),
            "bob@example.com");

    EdgeDecisionRuleStore store =
        newTestStore(
            List.of(
                emptyCreatedBy,
                contextualRule(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    AuditFilter auditFilter = AuditFilter.newBuilder().setCreatedByContains("charlie").build();
    Filter filter = Filter.newBuilder().setAuditFilter(auditFilter).build();

    List<EdgeDecisionRuleRecord> results = store.getRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testLastModifiedByContainsExcludesNullLastModifiedBy() {
    EdgeDecisionRule ruleWithNullLastModifiedBy =
        EdgeDecisionRule.newBuilder().setId("id-1").build();
    ContextualConfigObject<EdgeDecisionRule> nullLastModifiedBy =
        new ContextualConfigObjectTestImpl<>(
            ruleWithNullLastModifiedBy,
            "id-1",
            Instant.ofEpochSecond(100),
            "alice@example.com",
            Instant.ofEpochSecond(200),
            null,
            Instant.ofEpochSecond(200),
            null);

    EdgeDecisionRuleStore store =
        newTestStore(
            List.of(
                nullLastModifiedBy,
                contextualRule(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    AuditFilter auditFilter = AuditFilter.newBuilder().setLastUpdatedByUserContains("dave").build();
    Filter filter = Filter.newBuilder().setAuditFilter(auditFilter).build();

    List<EdgeDecisionRuleRecord> results = store.getRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testLastModifiedByContainsExcludesEmptyLastModifiedBy() {
    EdgeDecisionRule ruleWithEmptyLastModifiedBy =
        EdgeDecisionRule.newBuilder().setId("id-1").build();
    ContextualConfigObject<EdgeDecisionRule> emptyLastModifiedBy =
        new ContextualConfigObjectTestImpl<>(
            ruleWithEmptyLastModifiedBy,
            "id-1",
            Instant.ofEpochSecond(100),
            "alice@example.com",
            Instant.ofEpochSecond(200),
            "",
            Instant.ofEpochSecond(200),
            "");

    EdgeDecisionRuleStore store =
        newTestStore(
            List.of(
                emptyLastModifiedBy,
                contextualRule(
                    "id-2",
                    Instant.ofEpochSecond(100),
                    "charlie@example.com",
                    Instant.ofEpochSecond(200),
                    "dave@example.com")));

    AuditFilter auditFilter = AuditFilter.newBuilder().setLastUpdatedByUserContains("dave").build();
    Filter filter = Filter.newBuilder().setAuditFilter(auditFilter).build();

    List<EdgeDecisionRuleRecord> results = store.getRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testLastModifiedByContainsEmptyDoesNotFilter() {
    EdgeDecisionRuleStore store =
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

    AuditFilter auditFilter = AuditFilter.newBuilder().setLastUpdatedByUserContains("").build();
    Filter filter = Filter.newBuilder().setAuditFilter(auditFilter).build();

    List<EdgeDecisionRuleRecord> results = store.getRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(2, results.size());
  }

  @Test
  void testCreatedRangeWithEndOnlyExcludesTimestampAfterEnd() {
    EdgeDecisionRuleStore store =
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

    AuditFilter auditFilter = AuditFilter.newBuilder().setCreatedRange(range).build();
    Filter filter = Filter.newBuilder().setAuditFilter(auditFilter).build();

    List<EdgeDecisionRuleRecord> results = store.getRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testModifiedRangeWithEndOnlyExcludesTimestampAfterEnd() {
    EdgeDecisionRuleStore store =
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

    AuditFilter auditFilter = AuditFilter.newBuilder().setUpdatedRange(range).build();
    Filter filter = Filter.newBuilder().setAuditFilter(auditFilter).build();

    List<EdgeDecisionRuleRecord> results = store.getRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testCreatedRangeExcludesTimestampBeforeStart() {
    EdgeDecisionRuleStore store =
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

    AuditFilter auditFilter = AuditFilter.newBuilder().setCreatedRange(range).build();
    Filter filter = Filter.newBuilder().setAuditFilter(auditFilter).build();

    List<EdgeDecisionRuleRecord> results = store.getRuleRecords(REQUEST_CONTEXT, filter);
    assertEquals(1, results.size());
    assertEquals("id-2", results.get(0).getRule().getId());
  }

  @Test
  void testGetRuleRecordsBuildsAuditDetails() {
    EdgeDecisionRuleStore store =
        newTestStore(
            List.of(
                contextualRule(
                    "id-1",
                    Instant.ofEpochSecond(100),
                    "alice@example.com",
                    Instant.ofEpochSecond(200),
                    "bob@example.com"),
                contextualRule("id-2", Instant.EPOCH, "", Instant.EPOCH, "")));

    List<EdgeDecisionRuleRecord> records =
        store.getRuleRecords(REQUEST_CONTEXT, Filter.getDefaultInstance());
    Map<String, EdgeDecisionRuleRecord> recordsById =
        records.stream().collect(Collectors.toMap(r -> r.getRule().getId(), Function.identity()));

    assertTrue(recordsById.containsKey("id-1"));
    EdgeDecisionRuleRecord record1 = recordsById.get("id-1");
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
    EdgeDecisionRuleRecord record2 = recordsById.get("id-2");
    assertTrue(record2.hasAuditDetails());
    assertFalse(record2.getAuditDetails().hasCreationDetails());
    assertFalse(record2.getAuditDetails().hasLastUserUpdateDetails());
  }

  private static ContextualConfigObject<EdgeDecisionRule> contextualRule(
      String id,
      Instant creationTimestamp,
      String createdByEmail,
      Instant lastUserUpdateTimestamp,
      String lastUserUpdateEmail) {
    EdgeDecisionRule rule = EdgeDecisionRule.newBuilder().setId(id).build();
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

  private static EdgeDecisionRuleStore newTestStore(
      List<ContextualConfigObject<EdgeDecisionRule>> objects) {
    TimestampConverter timestampConverter = new TimestampConverter();
    FilterEvaluator filterEvaluator = new FilterEvaluator(timestampConverter);

    return new TestEdgeDecisionRuleStore(objects, filterEvaluator, timestampConverter);
  }

  private static class TestEdgeDecisionRuleStore extends EdgeDecisionRuleStore {

    private final List<ContextualConfigObject<EdgeDecisionRule>> objects;

    TestEdgeDecisionRuleStore(
        List<ContextualConfigObject<EdgeDecisionRule>> objects,
        FilterEvaluator filterEvaluator,
        TimestampConverter timestampConverter) {
      super(
          mock(ConfigServiceBlockingStub.class),
          mock(ConfigChangeEventGenerator.class),
          filterEvaluator,
          timestampConverter);
      this.objects = objects;
    }

    @Override
    public List<ContextualConfigObject<EdgeDecisionRule>> getAllObjects(RequestContext context) {
      return objects;
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
