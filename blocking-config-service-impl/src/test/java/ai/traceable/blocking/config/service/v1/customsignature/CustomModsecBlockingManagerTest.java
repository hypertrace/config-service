package ai.traceable.blocking.config.service.v1.customsignature;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.common.customsignature.CustomSignatureBlobFetcher;
import ai.traceable.blocking.config.service.v1.CustomModsecBlockingRules;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class CustomModsecBlockingManagerTest {
  private static final String TENANT_ID = "tenant-id";
  private static final Optional<String> environmentId = Optional.of("env-id");
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final String TEST_BLOB = "Tester rule blob";

  @Test
  void testBlobFetcher() {
    CustomSignatureBlobFetcher customSignatureBlobFetcher = mock(CustomSignatureBlobFetcher.class);
    doReturn(TEST_BLOB)
        .when(customSignatureBlobFetcher)
        .getEnabledCustomSignatureRulesBlob(
            REQUEST_CONTEXT, CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3, environmentId);

    UuidGenerator uuidGenerator = new UuidGenerator();

    CustomModsecBlockingManager customModsecBlockingManager =
        new DefaultCustomModsecBlockingManager(customSignatureBlobFetcher, uuidGenerator);

    // When hash does not match we expect the blob
    assertEquals(
        CustomModsecBlockingRules.newBuilder()
            .setHash(uuidGenerator.generateId(TEST_BLOB))
            .setCustomModsecRulesBlob(TEST_BLOB)
            .build(),
        customModsecBlockingManager.getEnabledBlockingRules(REQUEST_CONTEXT, "", environmentId));

    // When hash matches we don't expect the blob
    assertEquals(
        CustomModsecBlockingRules.newBuilder().setHash(uuidGenerator.generateId(TEST_BLOB)).build(),
        customModsecBlockingManager.getEnabledBlockingRules(
            REQUEST_CONTEXT, uuidGenerator.generateId(TEST_BLOB), environmentId));
  }
}
