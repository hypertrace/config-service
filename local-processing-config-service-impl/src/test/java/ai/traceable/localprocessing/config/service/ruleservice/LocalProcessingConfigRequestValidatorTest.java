package ai.traceable.localprocessing.config.service.ruleservice;

import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import ai.traceable.localprocessing.config.service.v1.UpdateDefaultProtectionModeRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class LocalProcessingConfigRequestValidatorTest {
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenant-1");

  @Test
  void validateDefaultProtectionMode() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LocalProcessingConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateDefaultProtectionModeRequest.newBuilder()
                    .setDefaultProtectionMode(ProtectionMode.PROTECTION_MODE_UNSPECIFIED)
                    .build()));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            LocalProcessingConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                UpdateDefaultProtectionModeRequest.newBuilder()
                    .setDefaultProtectionMode(ProtectionMode.UNRECOGNIZED)
                    .build()));
  }
}
