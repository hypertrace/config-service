package ai.traceable.ast.hooks.config.service.handlers;

import static ai.traceable.ast.hooks.config.service.v1.TestStatus.TEST_STATUS_PENDING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.ast.hooks.config.service.store.AstHooksConfigStore;
import ai.traceable.ast.hooks.config.service.v1.AstHook;
import ai.traceable.ast.hooks.config.service.v1.AstHookDetails;
import ai.traceable.ast.hooks.config.service.v1.UpdateAstHookRequest;
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
class UpdateAstHookHandlerTest {

  @Mock AstHooksConfigStore mockConfigStore;
  @Mock RequestContext mockRequestContext;
  @Mock ContextualConfigObject<AstHook> mockConfigObject;

  UpdateAstHookHandler updateAstHookHandler;

  @BeforeEach
  void setUp() {
    updateAstHookHandler =
        new UpdateAstHookHandler(mockConfigStore, new UpdateAstHookConfigHandler());
  }

  @Test
  void testUpdateHookWithTestIdSetsLastTestStatusToPending() {
    // Given
    final AstHook existingHook =
        AstHook.newBuilder()
            .setId("hook-1")
            .setHookDetails(AstHookDetails.newBuilder().setName("test-hook").build())
            .build();
    when(mockConfigStore.getData(mockRequestContext, "hook-1"))
        .thenReturn(Optional.of(existingHook));
    when(mockConfigStore.upsertObject(eq(mockRequestContext), any(AstHook.class)))
        .thenReturn(mockConfigObject);
    when(mockConfigObject.getData()).thenReturn(existingHook);

    // When
    updateAstHookHandler.updateHook(
        UpdateAstHookRequest.newBuilder().setId("hook-1").setHookTestId("test-123").build(),
        mockRequestContext);

    // Then
    final ArgumentCaptor<AstHook> hookCaptor = ArgumentCaptor.forClass(AstHook.class);
    verify(mockConfigStore).upsertObject(eq(mockRequestContext), hookCaptor.capture());
    final AstHook upsertedHook = hookCaptor.getValue();
    assertEquals("test-123", upsertedHook.getAstHookTestId());
    assertTrue(upsertedHook.hasLastTestStatus());
    assertEquals(TEST_STATUS_PENDING, upsertedHook.getLastTestStatus());
  }

  @Test
  void testUpdateHookWithoutTestIdDoesNotSetLastTestStatus() {
    // Given
    final AstHook existingHook =
        AstHook.newBuilder()
            .setId("hook-2")
            .setHookDetails(AstHookDetails.newBuilder().setName("test-hook").build())
            .build();
    when(mockConfigStore.getData(mockRequestContext, "hook-2"))
        .thenReturn(Optional.of(existingHook));
    when(mockConfigStore.upsertObject(eq(mockRequestContext), any(AstHook.class)))
        .thenReturn(mockConfigObject);
    when(mockConfigObject.getData()).thenReturn(existingHook);

    // When
    updateAstHookHandler.updateHook(
        UpdateAstHookRequest.newBuilder().setId("hook-2").setName("renamed-hook").build(),
        mockRequestContext);

    // Then
    final ArgumentCaptor<AstHook> hookCaptor = ArgumentCaptor.forClass(AstHook.class);
    verify(mockConfigStore).upsertObject(eq(mockRequestContext), hookCaptor.capture());
    final AstHook upsertedHook = hookCaptor.getValue();
    assertFalse(upsertedHook.hasLastTestStatus());
    assertFalse(upsertedHook.hasAstHookTestId());
  }
}
