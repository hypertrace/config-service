package ai.traceable.audit.utils;

import static ai.traceable.audit.utils.AuditContextualObjectUtils.TRACEABLE;
import static ai.traceable.audit.utils.AuditContextualObjectUtils.contextualObjectWithDefaultTraceableAuditInfo;
import static ai.traceable.audit.utils.AuditContextualObjectUtils.enrichWithDefaultAuditInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.time.Instant;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class AuditContextualObjectUtilsTest {

  @Nested
  class ContextualObjectWithDefaultTraceableAuditInfo {

    @Test
    void shouldSetTraceableAsCreatedByEmail() {
      String testData = "test-data";
      String testId = "test-id";

      ContextualConfigObject<String> result =
          contextualObjectWithDefaultTraceableAuditInfo(testData, testId);

      assertEquals(TRACEABLE, result.getCreatedByEmail());
    }

    @Test
    void shouldSetTraceableAsLastUserUpdateEmail() {
      String testData = "test-data";
      String testId = "test-id";

      ContextualConfigObject<String> result =
          contextualObjectWithDefaultTraceableAuditInfo(testData, testId);

      assertEquals(TRACEABLE, result.getLastUserUpdateEmail());
    }

    @Test
    void shouldSetTraceableAsLastUpdateEmail() {
      String testData = "test-data";
      String testId = "test-id";

      ContextualConfigObject<String> result =
          contextualObjectWithDefaultTraceableAuditInfo(testData, testId);

      assertEquals(TRACEABLE, result.getLastUpdateEmail());
    }

    @Test
    void shouldSetContextToProvidedId() {
      String testData = "test-data";
      String testId = "test-id";

      ContextualConfigObject<String> result =
          contextualObjectWithDefaultTraceableAuditInfo(testData, testId);

      assertEquals(testId, result.getContext());
    }

    @Test
    void shouldSetDataToProvidedData() {
      String testData = "test-data";
      String testId = "test-id";

      ContextualConfigObject<String> result =
          contextualObjectWithDefaultTraceableAuditInfo(testData, testId);

      assertEquals(testData, result.getData());
    }

    @Test
    void shouldNotSetTimestamps() {
      String testData = "test-data";
      String testId = "test-id";

      ContextualConfigObject<String> result =
          contextualObjectWithDefaultTraceableAuditInfo(testData, testId);

      assertNull(result.getCreationTimestamp());
      assertNull(result.getLastUpdatedTimestamp());
      assertNull(result.getLastUserUpdateTimestamp());
    }
  }

  @Nested
  class EnrichWithDefaultAuditInfo {

    @Test
    void shouldReturnPersistedRuleWhenDefaultIsNull() {
      ContextualConfigObject<String> persistedRule =
          ContextualObjectImpl.<String>builder()
              .context("persisted-id")
              .data("persisted-data")
              .createdByEmail("user@example.com")
              .build();

      ContextualConfigObject<String> result = enrichWithDefaultAuditInfo(persistedRule, null);

      assertSame(persistedRule, result);
    }

    @Test
    void shouldMergeAuditInfoWhenDefaultIsNotNull() {
      ContextualConfigObject<String> persistedRule =
          ContextualObjectImpl.<String>builder()
              .context("persisted-id")
              .data("persisted-data")
              .createdByEmail("user@example.com")
              .build();

      ContextualConfigObject<String> defaultRule =
          ContextualObjectImpl.<String>builder()
              .context("default-id")
              .data("default-data")
              .createdByEmail(TRACEABLE)
              .lastUserUpdateEmail(TRACEABLE)
              .build();

      ContextualConfigObject<String> result =
          enrichWithDefaultAuditInfo(persistedRule, defaultRule);

      // Should use default's createdByEmail
      assertEquals(TRACEABLE, result.getCreatedByEmail());
      // Should preserve persisted's data and context
      assertEquals("persisted-data", result.getData());
      assertEquals("persisted-id", result.getContext());
    }
  }

  @Nested
  class MergeAuditInfo {

    @Test
    void shouldUseDefaultCreatedByEmail() {
      ContextualConfigObject<String> persistedRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .createdByEmail("user@example.com")
              .lastUserUpdateEmail("user@example.com")
              .build();

      ContextualConfigObject<String> defaultRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .createdByEmail(TRACEABLE)
              .lastUserUpdateEmail(TRACEABLE)
              .build();

      ContextualConfigObject<String> result =
          enrichWithDefaultAuditInfo(persistedRule, defaultRule);

      assertEquals(TRACEABLE, result.getCreatedByEmail());
    }

    @Test
    void shouldUseDefaultCreationTimestamp() {
      Instant defaultCreationTime = Instant.now().minusSeconds(86400);
      Instant persistedCreationTime = Instant.now();

      ContextualConfigObject<String> persistedRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .creationTimestamp(persistedCreationTime)
              .lastUserUpdateEmail("user@example.com")
              .build();

      ContextualConfigObject<String> defaultRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .creationTimestamp(defaultCreationTime)
              .lastUserUpdateEmail(TRACEABLE)
              .build();

      ContextualConfigObject<String> result =
          enrichWithDefaultAuditInfo(persistedRule, defaultRule);

      assertEquals(defaultCreationTime, result.getCreationTimestamp());
    }

    @Test
    void shouldPreservePersistedLastUserUpdateEmailWhenNotNullOrUnknown() {
      String persistedEmail = "user@example.com";
      Instant persistedTimestamp = Instant.now();

      ContextualConfigObject<String> persistedRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUserUpdateEmail(persistedEmail)
              .lastUserUpdateTimestamp(persistedTimestamp)
              .build();

      ContextualConfigObject<String> defaultRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUserUpdateEmail(TRACEABLE)
              .lastUserUpdateTimestamp(Instant.now().minusSeconds(86400))
              .build();

      ContextualConfigObject<String> result =
          enrichWithDefaultAuditInfo(persistedRule, defaultRule);

      assertEquals(persistedEmail, result.getLastUserUpdateEmail());
      assertEquals(persistedTimestamp, result.getLastUserUpdateTimestamp());
    }

    @Test
    void shouldFallbackToDefaultLastUserUpdateEmailWhenPersistedIsNull() {
      Instant defaultTimestamp = Instant.now().minusSeconds(86400);

      ContextualConfigObject<String> persistedRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUserUpdateEmail(null)
              .lastUserUpdateTimestamp(null)
              .build();

      ContextualConfigObject<String> defaultRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUserUpdateEmail(TRACEABLE)
              .lastUserUpdateTimestamp(defaultTimestamp)
              .build();

      ContextualConfigObject<String> result =
          enrichWithDefaultAuditInfo(persistedRule, defaultRule);

      assertEquals(TRACEABLE, result.getLastUserUpdateEmail());
      assertEquals(defaultTimestamp, result.getLastUserUpdateTimestamp());
    }

    @Test
    void shouldFallbackToDefaultLastUserUpdateEmailWhenPersistedIsEmpty() {
      Instant defaultTimestamp = Instant.now().minusSeconds(86400);

      ContextualConfigObject<String> persistedRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUserUpdateEmail("")
              .lastUserUpdateTimestamp(null)
              .build();

      ContextualConfigObject<String> defaultRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUserUpdateEmail(TRACEABLE)
              .lastUserUpdateTimestamp(defaultTimestamp)
              .build();

      ContextualConfigObject<String> result =
          enrichWithDefaultAuditInfo(persistedRule, defaultRule);

      assertEquals(TRACEABLE, result.getLastUserUpdateEmail());
      assertEquals(defaultTimestamp, result.getLastUserUpdateTimestamp());
    }

    @Test
    void shouldFallbackToDefaultLastUserUpdateEmailWhenPersistedIsUnknown() {
      Instant defaultTimestamp = Instant.now().minusSeconds(86400);

      ContextualConfigObject<String> persistedRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUserUpdateEmail("Unknown")
              .lastUserUpdateTimestamp(Instant.now())
              .build();

      ContextualConfigObject<String> defaultRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUserUpdateEmail(TRACEABLE)
              .lastUserUpdateTimestamp(defaultTimestamp)
              .build();

      ContextualConfigObject<String> result =
          enrichWithDefaultAuditInfo(persistedRule, defaultRule);

      assertEquals(TRACEABLE, result.getLastUserUpdateEmail());
      assertEquals(defaultTimestamp, result.getLastUserUpdateTimestamp());
    }

    @Test
    void shouldFallbackToDefaultLastUserUpdateEmailWhenPersistedIsUnknownCaseInsensitive() {
      Instant defaultTimestamp = Instant.now().minusSeconds(86400);

      ContextualConfigObject<String> persistedRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUserUpdateEmail("UNKNOWN")
              .lastUserUpdateTimestamp(Instant.now())
              .build();

      ContextualConfigObject<String> defaultRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUserUpdateEmail(TRACEABLE)
              .lastUserUpdateTimestamp(defaultTimestamp)
              .build();

      ContextualConfigObject<String> result =
          enrichWithDefaultAuditInfo(persistedRule, defaultRule);

      assertEquals(TRACEABLE, result.getLastUserUpdateEmail());
      assertEquals(defaultTimestamp, result.getLastUserUpdateTimestamp());
    }

    @Test
    void shouldPreservePersistedLastUpdatedTimestamp() {
      Instant persistedLastUpdatedTimestamp = Instant.now();
      Instant defaultLastUpdatedTimestamp = Instant.now().minusSeconds(86400);

      ContextualConfigObject<String> persistedRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUpdatedTimestamp(persistedLastUpdatedTimestamp)
              .lastUserUpdateEmail("user@example.com")
              .build();

      ContextualConfigObject<String> defaultRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUpdatedTimestamp(defaultLastUpdatedTimestamp)
              .lastUserUpdateEmail(TRACEABLE)
              .build();

      ContextualConfigObject<String> result =
          enrichWithDefaultAuditInfo(persistedRule, defaultRule);

      assertEquals(persistedLastUpdatedTimestamp, result.getLastUpdatedTimestamp());
    }

    @Test
    void shouldPreservePersistedLastUpdateEmail() {
      String persistedLastUpdateEmail = "system@example.com";

      ContextualConfigObject<String> persistedRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUpdateEmail(persistedLastUpdateEmail)
              .lastUserUpdateEmail("user@example.com")
              .build();

      ContextualConfigObject<String> defaultRule =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUpdateEmail(TRACEABLE)
              .lastUserUpdateEmail(TRACEABLE)
              .build();

      ContextualConfigObject<String> result =
          enrichWithDefaultAuditInfo(persistedRule, defaultRule);

      assertEquals(persistedLastUpdateEmail, result.getLastUpdateEmail());
    }

    @Test
    void shouldPreservePersistedDataAndContext() {
      String persistedData = "persisted-data";
      String persistedContext = "persisted-context";

      ContextualConfigObject<String> persistedRule =
          ContextualObjectImpl.<String>builder()
              .context(persistedContext)
              .data(persistedData)
              .lastUserUpdateEmail("user@example.com")
              .build();

      ContextualConfigObject<String> defaultRule =
          ContextualObjectImpl.<String>builder()
              .context("default-context")
              .data("default-data")
              .lastUserUpdateEmail(TRACEABLE)
              .build();

      ContextualConfigObject<String> result =
          enrichWithDefaultAuditInfo(persistedRule, defaultRule);

      assertEquals(persistedData, result.getData());
      assertEquals(persistedContext, result.getContext());
    }
  }
}
