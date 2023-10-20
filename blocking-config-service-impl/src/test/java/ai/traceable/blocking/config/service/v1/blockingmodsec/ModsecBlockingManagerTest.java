package ai.traceable.blocking.config.service.v1.blockingmodsec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.blocking.config.service.common.modsec.BlockingModsecBlobFetcher;
import ai.traceable.blocking.config.service.v1.SafeCrsBlockingRules;
import ai.traceable.config.utils.UuidGenerator;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ModsecBlockingManagerTest {
  private static final String TENANT_ID = "tenant-id";
  private static final Optional<String> environmentId = Optional.of("env-id");
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final String TEST_BLOB = "Tester rule blob";
  private final UuidGenerator uuidGenerator = new UuidGenerator();
  private BlockingModsecBlobFetcher blockingModsecBlobFetcher;
  private ModsecBlockingManager modsecBlockingManager;

  @BeforeEach
  void setup() {
    blockingModsecBlobFetcher = mock(BlockingModsecBlobFetcher.class);
    modsecBlockingManager =
        new DefaultModsecBlockingManager(blockingModsecBlobFetcher, uuidGenerator);
  }

  @Test
  void testDetectionRules() {
    SafeCrsBlockingRules expectedModsecRules =
        SafeCrsBlockingRules.newBuilder()
            .setSafeCrsRulesBlob(TEST_BLOB)
            .setHash(uuidGenerator.generateId(TEST_BLOB))
            .build();

    when(blockingModsecBlobFetcher.getEnabledRulesBlob(
            REQUEST_CONTEXT, ModsecRuleVersion.MODSEC_RULE_VERSION_V3, environmentId))
        .thenReturn(TEST_BLOB);

    // When hash does not match we expect the blob
    assertEquals(
        expectedModsecRules,
        modsecBlockingManager.getEnabledBlockingRules(REQUEST_CONTEXT, "", environmentId));

    // When hash matches we don't expect the blob
    assertEquals(
        SafeCrsBlockingRules.newBuilder().setHash(uuidGenerator.generateId(TEST_BLOB)).build(),
        modsecBlockingManager.getEnabledBlockingRules(
            REQUEST_CONTEXT, uuidGenerator.generateId(TEST_BLOB), environmentId));
  }

  @Test
  void propagateErrors() {
    when(blockingModsecBlobFetcher.getEnabledRulesBlob(any(), any(), any()))
        .thenThrow(RuntimeException.class);
    assertThrows(
        RuntimeException.class,
        () -> modsecBlockingManager.getEnabledBlockingRules(REQUEST_CONTEXT, "", environmentId));
  }
}
