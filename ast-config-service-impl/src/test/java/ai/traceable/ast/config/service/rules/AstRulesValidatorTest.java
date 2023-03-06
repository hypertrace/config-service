package ai.traceable.ast.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import ai.traceable.ast.config.service.v1.GetScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.ScanPurgeConfig;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import com.google.protobuf.Duration;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AstRulesValidatorTest {
  private static final String TENANT_ID = "default-tenant";
  private AstRulesValidator rulesValidator = new AstRulesValidator();
  private RequestContext mockRequestContext = Mockito.mock(RequestContext.class);

  @Test
  void validateUpdateRequest() {

    // invalid request context
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext, UpdateScanPurgeConfigRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID));

    // no purge config
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext, UpdateScanPurgeConfigRequest.getDefaultInstance()));

    // no purge duration
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                UpdateScanPurgeConfigRequest.newBuilder()
                    .setPurgeConfig(ScanPurgeConfig.getDefaultInstance())
                    .build()));

    // valid request
    assertDoesNotThrow(
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext,
                UpdateScanPurgeConfigRequest.newBuilder()
                    .setPurgeConfig(
                        ScanPurgeConfig.newBuilder()
                            .setPurgeDuration(Duration.newBuilder().setSeconds(1)))
                    .build()));
  }

  @Test
  void validateGetRequest() {

    // invalid request context
    assertThrows(
        RuntimeException.class,
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext, GetScanPurgeConfigRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID));

    // valid request
    assertDoesNotThrow(
        () ->
            rulesValidator.validateOrThrow(
                mockRequestContext, GetScanPurgeConfigRequest.getDefaultInstance()));
  }
}
