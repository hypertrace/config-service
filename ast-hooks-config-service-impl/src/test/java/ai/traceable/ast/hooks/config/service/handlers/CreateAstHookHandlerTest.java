package ai.traceable.ast.hooks.config.service.handlers;

import static ai.traceable.ast.hooks.config.service.v1.TestStatus.TEST_STATUS_PASSED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.ast.hooks.config.service.store.AstHooksConfigStore;
import ai.traceable.ast.hooks.config.service.store.AstHooksTestConfigStore;
import ai.traceable.ast.hooks.config.service.v1.AstHook;
import ai.traceable.ast.hooks.config.service.v1.AstHookDetails;
import ai.traceable.ast.hooks.config.service.v1.AstHookTest;
import ai.traceable.ast.hooks.config.service.v1.CreateAstHookRequest;
import ai.traceable.config.utils.UuidGenerator;
import java.util.Optional;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CreateAstHookHandlerTest {

  @Mock UuidGenerator mockUuidGenerator;
  @Mock AstHooksConfigStore mockConfigStore;
  @Mock AstHooksTestConfigStore mockTestConfigStore;
  @Mock RequestContext mockRequestContext;
  @Mock ContextualConfigObject<AstHook> mockConfigObject;

  CreateAstHookHandler createAstHookHandler;

  @BeforeEach
  void setUp() {
    createAstHookHandler =
        new CreateAstHookHandler(mockUuidGenerator, mockConfigStore, mockTestConfigStore);
  }

  @Test
  void testCreateHookWithTestIdSetsStatusFromTestConfig() {
    // Given
    when(mockUuidGenerator.generateRandomId()).thenReturn("hook-1");
    when(mockTestConfigStore.getData(mockRequestContext, "test-123"))
        .thenReturn(
            Optional.of(
                AstHookTest.newBuilder()
                    .setId("test-123")
                    .setTestStatus(TEST_STATUS_PASSED)
                    .build()));
    when(mockConfigStore.upsertObject(eq(mockRequestContext), any(AstHook.class)))
        .thenReturn(mockConfigObject);
    when(mockConfigObject.getData()).thenReturn(AstHook.getDefaultInstance());

    // When
    createAstHookHandler.createHook(
        CreateAstHookRequest.newBuilder()
            .setHookDetails(AstHookDetails.newBuilder().setName("my-hook").build())
            .setHookTestId("test-123")
            .build(),
        mockRequestContext);

    // Then
    final ArgumentCaptor<AstHook> hookCaptor = ArgumentCaptor.forClass(AstHook.class);
    verify(mockConfigStore).upsertObject(eq(mockRequestContext), hookCaptor.capture());
    final AstHook createdHook = hookCaptor.getValue();
    assertEquals("test-123", createdHook.getAstHookTestId());
    assertTrue(createdHook.hasLastTestStatus());
    assertEquals(TEST_STATUS_PASSED, createdHook.getLastTestStatus());
  }

  @Test
  void testCreateHookWithTestIdWhenTestConfigMissing() {
    // Given
    when(mockUuidGenerator.generateRandomId()).thenReturn("hook-2");
    when(mockTestConfigStore.getData(mockRequestContext, "test-456")).thenReturn(Optional.empty());
    when(mockConfigStore.upsertObject(eq(mockRequestContext), any(AstHook.class)))
        .thenReturn(mockConfigObject);
    when(mockConfigObject.getData()).thenReturn(AstHook.getDefaultInstance());

    // When
    createAstHookHandler.createHook(
        CreateAstHookRequest.newBuilder()
            .setHookDetails(AstHookDetails.newBuilder().setName("my-hook").build())
            .setHookTestId("test-456")
            .build(),
        mockRequestContext);

    // Then
    final ArgumentCaptor<AstHook> hookCaptor = ArgumentCaptor.forClass(AstHook.class);
    verify(mockConfigStore).upsertObject(eq(mockRequestContext), hookCaptor.capture());
    final AstHook createdHook = hookCaptor.getValue();
    assertEquals("test-456", createdHook.getAstHookTestId());
    assertFalse(createdHook.hasLastTestStatus());
  }

  @Test
  void testCreateHookWithoutTestId() {
    // Given
    when(mockUuidGenerator.generateRandomId()).thenReturn("hook-3");
    when(mockConfigStore.upsertObject(eq(mockRequestContext), any(AstHook.class)))
        .thenReturn(mockConfigObject);
    when(mockConfigObject.getData()).thenReturn(AstHook.getDefaultInstance());

    // When
    createAstHookHandler.createHook(
        CreateAstHookRequest.newBuilder()
            .setHookDetails(AstHookDetails.newBuilder().setName("my-hook").build())
            .build(),
        mockRequestContext);

    // Then
    final ArgumentCaptor<AstHook> hookCaptor = ArgumentCaptor.forClass(AstHook.class);
    verify(mockConfigStore).upsertObject(eq(mockRequestContext), hookCaptor.capture());
    final AstHook createdHook = hookCaptor.getValue();
    assertFalse(createdHook.hasAstHookTestId());
    assertFalse(createdHook.hasLastTestStatus());
  }
}
