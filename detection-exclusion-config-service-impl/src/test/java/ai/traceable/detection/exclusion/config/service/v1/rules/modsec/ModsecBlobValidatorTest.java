package ai.traceable.detection.exclusion.config.service.v1.rules.modsec;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.modsecurity.utils.ModsecRuleEngineUtils;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class ModsecBlobValidatorTest {
  private static final RequestContext requestContext = RequestContext.forTenantId("tenantId");

  private ModsecBlobValidator validator;
  private UuidGenerator uuidGenerator;

  @BeforeEach
  void setUp() {
    uuidGenerator = Mockito.mock(UuidGenerator.class);
    validator = new ModsecBlobValidator(uuidGenerator);
  }

  @Test
  void testValidate_ValidBlob() {
    try (MockedStatic<ModsecRuleEngineUtils> mockModsecUtils =
        mockStatic(ModsecRuleEngineUtils.class)) {
      mockModsecUtils.when(() -> ModsecRuleEngineUtils.validate(anyString())).thenReturn(Status.OK);

      when(uuidGenerator.generateId(anyString())).thenReturn("valid-blob-id");

      boolean result = validator.validate(requestContext, "valid-modsec-blob", "test-service");

      assertTrue(result);
    }
  }

  @Test
  void testValidate_InvalidBlob() {
    try (MockedStatic<ModsecRuleEngineUtils> mockModsecUtils =
        mockStatic(ModsecRuleEngineUtils.class)) {
      mockModsecUtils
          .when(() -> ModsecRuleEngineUtils.validate(anyString()))
          .thenReturn(Status.INVALID_ARGUMENT);

      RequestContext requestContext = RequestContext.forTenantId("test-tenant");
      when(uuidGenerator.generateId(anyString())).thenReturn("invalid-blob-id");

      boolean result = validator.validate(requestContext, "invalid-modsec-blob", "test-service");

      assertFalse(result);
    }
  }

  @Test
  void testValidate_UnknownBlob() {
    try (MockedStatic<ModsecRuleEngineUtils> mockModsecUtils =
        mockStatic(ModsecRuleEngineUtils.class)) {
      mockModsecUtils
          .when(() -> ModsecRuleEngineUtils.validate(anyString()))
          .thenReturn(Status.UNKNOWN);

      when(uuidGenerator.generateId(anyString())).thenReturn("valid-blob-id");
      boolean result = validator.validate(requestContext, "valid-modsec-blob", "test-service");
      assertTrue(result);
    }
  }

  @Test
  void testValidate_EmptyBlob() {
    RequestContext requestContext = RequestContext.forTenantId("test-tenant");

    boolean result = validator.validate(requestContext, "", "test-service");
    assertTrue(result);
  }

  @Test
  void testValidate_CachingBehavior() {
    try (MockedStatic<ModsecRuleEngineUtils> mockModsecUtils =
        mockStatic(ModsecRuleEngineUtils.class)) {
      mockModsecUtils.when(() -> ModsecRuleEngineUtils.validate(anyString())).thenReturn(Status.OK);

      String modsecRuleBlob = "cached-modsec-blob";
      String modsecBlobId = "cached-blob-id";

      // First call: Expect ModsecRuleEngineUtils.validate to be invoked
      when(uuidGenerator.generateId(modsecRuleBlob)).thenReturn(modsecBlobId);
      boolean firstCallResult = validator.validate(requestContext, modsecRuleBlob, "test-service");
      assertTrue(firstCallResult);

      // Second call with the same blob: Should not invoke ModsecRuleEngineUtils.validate again,
      // cached result should be returned
      mockModsecUtils
          .clearInvocations(); // Clear any previous invocations to check if the method is called
      // again
      boolean secondCallResult = validator.validate(requestContext, modsecRuleBlob, "test-service");

      assertTrue(secondCallResult);
      mockModsecUtils.verify(() -> ModsecRuleEngineUtils.validate(anyString()), Mockito.times(0));
    }
  }
}
