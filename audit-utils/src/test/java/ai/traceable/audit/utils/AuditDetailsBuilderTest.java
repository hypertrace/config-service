package ai.traceable.audit.utils;

import static ai.traceable.audit.utils.AuditDetailsBuilder.buildAuditDetails;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.config.commons.v1.AuditDetails;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Instant;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class AuditDetailsBuilderTest {

  private UserVisibleEmailConfig userVisibleEmailConfig;

  @BeforeEach
  void setUp() {
    Config config =
        ConfigFactory.parseString(
            "generic.config.service.customer.visible.excluded.email.patterns: []");
    userVisibleEmailConfig = new UserVisibleEmailConfig(config);
  }

  @Nested
  class BuildCreationDetails {

    @Test
    void shouldReturnCreationDetailsWithBothTimestampAndEmail() {
      Instant creationTimestamp = Instant.now();
      String createdByEmail = "user@example.com";

      ContextualConfigObject<String> contextual =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .creationTimestamp(creationTimestamp)
              .createdByEmail(createdByEmail)
              .build();

      AuditDetails result = buildAuditDetails(contextual, userVisibleEmailConfig);

      assertTrue(result.hasCreationDetails());
      assertEquals(
          creationTimestamp.getEpochSecond(),
          result.getCreationDetails().getCreatedAt().getSeconds());
      assertEquals(createdByEmail, result.getCreationDetails().getCreatedBy());
    }

    @Test
    void shouldReturnCreationDetailsWithOnlyEmail() {
      String createdByEmail = "user@example.com";

      ContextualConfigObject<String> contextual =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .creationTimestamp(null)
              .createdByEmail(createdByEmail)
              .build();

      AuditDetails result = buildAuditDetails(contextual, userVisibleEmailConfig);

      assertTrue(result.hasCreationDetails());
      assertEquals(0, result.getCreationDetails().getCreatedAt().getSeconds());
      assertEquals(createdByEmail, result.getCreationDetails().getCreatedBy());
    }

    @Test
    void shouldReturnCreationDetailsWithOnlyTimestamp() {
      Instant creationTimestamp = Instant.now();

      ContextualConfigObject<String> contextual =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .creationTimestamp(creationTimestamp)
              .createdByEmail(null)
              .build();

      AuditDetails result = buildAuditDetails(contextual, userVisibleEmailConfig);

      assertTrue(result.hasCreationDetails());
      assertEquals(
          creationTimestamp.getEpochSecond(),
          result.getCreationDetails().getCreatedAt().getSeconds());
      assertTrue(result.getCreationDetails().getCreatedBy().isEmpty());
    }

    @Test
    void shouldNotReturnCreationDetailsWhenNoTimestampAndNoEmail() {
      ContextualConfigObject<String> contextual =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .creationTimestamp(null)
              .createdByEmail(null)
              .build();

      AuditDetails result = buildAuditDetails(contextual, userVisibleEmailConfig);

      assertFalse(result.hasCreationDetails());
    }

    @Test
    void shouldNotReturnCreationDetailsWhenTimestampIsZeroAndEmailIsEmpty() {
      ContextualConfigObject<String> contextual =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .creationTimestamp(Instant.EPOCH)
              .createdByEmail("")
              .build();

      AuditDetails result = buildAuditDetails(contextual, userVisibleEmailConfig);

      assertFalse(result.hasCreationDetails());
    }
  }

  @Nested
  class BuildLastUpdateDetails {

    @Test
    void shouldReturnLastUpdateDetailsWithBothTimestampAndEmail() {
      Instant lastUserUpdateTimestamp = Instant.now();
      String lastUserUpdateEmail = "user@example.com";

      ContextualConfigObject<String> contextual =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUserUpdateTimestamp(lastUserUpdateTimestamp)
              .lastUserUpdateEmail(lastUserUpdateEmail)
              .build();

      AuditDetails result = buildAuditDetails(contextual, userVisibleEmailConfig);

      assertTrue(result.hasLastUserUpdateDetails());
      assertEquals(
          lastUserUpdateTimestamp.getEpochSecond(),
          result.getLastUserUpdateDetails().getUpdatedAt().getSeconds());
      assertEquals(lastUserUpdateEmail, result.getLastUserUpdateDetails().getUpdatedBy());
    }

    @Test
    void shouldReturnLastUpdateDetailsWithOnlyEmail() {
      String lastUserUpdateEmail = "user@example.com";

      ContextualConfigObject<String> contextual =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUserUpdateTimestamp(null)
              .lastUserUpdateEmail(lastUserUpdateEmail)
              .lastUpdatedTimestamp(null)
              .lastUpdateEmail(lastUserUpdateEmail)
              .build();

      AuditDetails result = buildAuditDetails(contextual, userVisibleEmailConfig);

      assertTrue(result.hasLastUserUpdateDetails());
      assertEquals(lastUserUpdateEmail, result.getLastUserUpdateDetails().getUpdatedBy());
    }

    @Test
    void shouldReturnLastUpdateDetailsWithOnlyTimestamp() {
      Instant lastUserUpdateTimestamp = Instant.now();

      ContextualConfigObject<String> contextual =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUserUpdateTimestamp(lastUserUpdateTimestamp)
              .lastUserUpdateEmail(null)
              .build();

      AuditDetails result = buildAuditDetails(contextual, userVisibleEmailConfig);

      assertTrue(result.hasLastUserUpdateDetails());
      assertEquals(
          lastUserUpdateTimestamp.getEpochSecond(),
          result.getLastUserUpdateDetails().getUpdatedAt().getSeconds());
    }

    @Test
    void shouldFallbackToLastUpdatedTimestampWhenLastUserUpdateTimestampIsNull() {
      Instant lastUpdatedTimestamp = Instant.now();
      String lastUpdateEmail = "system@example.com";

      ContextualConfigObject<String> contextual =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUserUpdateTimestamp(null)
              .lastUserUpdateEmail(null)
              .lastUpdatedTimestamp(lastUpdatedTimestamp)
              .lastUpdateEmail(lastUpdateEmail)
              .build();

      AuditDetails result = buildAuditDetails(contextual, userVisibleEmailConfig);

      assertTrue(result.hasLastUserUpdateDetails());
      assertEquals(
          lastUpdatedTimestamp.getEpochSecond(),
          result.getLastUserUpdateDetails().getUpdatedAt().getSeconds());
      assertEquals(lastUpdateEmail, result.getLastUserUpdateDetails().getUpdatedBy());
    }

    @Test
    void shouldReturnLastUpdateDetailsWithUnknownWhenNoTimestampAndNoEmail() {
      // When email is null, maskEmailIfNotVisible returns "Unknown"
      // So LastUpdateDetails will still be present with "Unknown" as updatedBy
      ContextualConfigObject<String> contextual =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .lastUserUpdateTimestamp(null)
              .lastUserUpdateEmail(null)
              .lastUpdatedTimestamp(null)
              .lastUpdateEmail(null)
              .build();

      AuditDetails result = buildAuditDetails(contextual, userVisibleEmailConfig);

      assertTrue(result.hasLastUserUpdateDetails());
      assertEquals("Unknown", result.getLastUserUpdateDetails().getUpdatedBy());
      assertEquals(0, result.getLastUserUpdateDetails().getUpdatedAt().getSeconds());
    }
  }

  @Nested
  class BuildAuditDetailsIntegration {

    @Test
    void shouldBuildCompleteAuditDetails() {
      Instant creationTimestamp = Instant.now().minusSeconds(86400);
      Instant lastUserUpdateTimestamp = Instant.now();
      String createdByEmail = "creator@example.com";
      String lastUserUpdateEmail = "updater@example.com";

      ContextualConfigObject<String> contextual =
          ContextualObjectImpl.<String>builder()
              .context("id")
              .data("data")
              .creationTimestamp(creationTimestamp)
              .createdByEmail(createdByEmail)
              .lastUserUpdateTimestamp(lastUserUpdateTimestamp)
              .lastUserUpdateEmail(lastUserUpdateEmail)
              .build();

      AuditDetails result = buildAuditDetails(contextual, userVisibleEmailConfig);

      assertTrue(result.hasCreationDetails());
      assertTrue(result.hasLastUserUpdateDetails());
      assertEquals(createdByEmail, result.getCreationDetails().getCreatedBy());
      assertEquals(lastUserUpdateEmail, result.getLastUserUpdateDetails().getUpdatedBy());
    }

    @Test
    void shouldHandleTraceableDefaultAuditInfo() {
      String traceableEmail = AuditContextualObjectUtils.TRACEABLE;

      ContextualConfigObject<String> contextual =
          AuditContextualObjectUtils.contextualObjectWithDefaultTraceableAuditInfo("data", "id");

      AuditDetails result = buildAuditDetails(contextual, userVisibleEmailConfig);

      assertTrue(result.hasCreationDetails());
      assertEquals(traceableEmail, result.getCreationDetails().getCreatedBy());
      // No creation timestamp set, so createdAt should be default (0)
      assertEquals(0, result.getCreationDetails().getCreatedAt().getSeconds());
    }
  }
}
