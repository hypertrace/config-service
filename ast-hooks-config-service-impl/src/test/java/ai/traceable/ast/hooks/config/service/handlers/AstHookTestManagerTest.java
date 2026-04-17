package ai.traceable.ast.hooks.config.service.handlers;

import static ai.traceable.ast.hooks.config.service.v1.TestStatus.TEST_STATUS_PASSED;
import static ai.traceable.ast.hooks.config.service.v1.TestStatus.TEST_STATUS_PENDING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.ast.hooks.config.service.store.AstHooksConfigStore;
import ai.traceable.ast.hooks.config.service.store.AstHooksTestConfigStore;
import ai.traceable.ast.hooks.config.service.v1.AstHook;
import ai.traceable.ast.hooks.config.service.v1.AstHookDetails;
import ai.traceable.ast.hooks.config.service.v1.AstHookTest;
import ai.traceable.ast.hooks.config.service.v1.UpdateAstHookTestRequest;
import ai.traceable.config.utils.UuidGenerator;
import java.util.List;
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
class AstHookTestManagerTest {

  @Mock UuidGenerator mockUuidGenerator;
  @Mock AstHooksTestConfigStore mockTestConfigStore;
  @Mock AstHooksConfigStore mockHooksConfigStore;
  @Mock RequestContext mockRequestContext;
  @Mock ContextualConfigObject<AstHookTest> mockTestConfigObject;
  @Mock ContextualConfigObject<AstHook> mockHookConfigObject;

  AstHookTestManager astHookTestManager;

  @BeforeEach
  void setUp() {
    astHookTestManager =
        new AstHookTestManager(
            mockUuidGenerator,
            mockTestConfigStore,
            new UpdateAstHookConfigHandler(),
            mockHooksConfigStore);
  }

  @Test
  void testUpdateHookTestStatusUpdatesParentHookLastTestStatus() {
    // Given
    final AstHookTest existingTest =
        AstHookTest.newBuilder().setId("test-1").setTestStatus(TEST_STATUS_PENDING).build();
    when(mockTestConfigStore.getData(mockRequestContext, "test-1"))
        .thenReturn(Optional.of(existingTest));
    when(mockTestConfigStore.upsertObject(eq(mockRequestContext), any(AstHookTest.class)))
        .thenReturn(mockTestConfigObject);
    when(mockTestConfigObject.getData()).thenReturn(existingTest);

    final AstHook hook =
        AstHook.newBuilder()
            .setId("hook-1")
            .setHookDetails(AstHookDetails.newBuilder().setName("my-hook").build())
            .setAstHookTestId("test-1")
            .setLastTestStatus(TEST_STATUS_PENDING)
            .build();
    when(mockHooksConfigStore.getAllConfigData(mockRequestContext)).thenReturn(List.of(hook));
    when(mockHooksConfigStore.upsertObject(eq(mockRequestContext), any(AstHook.class)))
        .thenReturn(mockHookConfigObject);

    // When
    astHookTestManager.updateHookTest(
        mockRequestContext,
        UpdateAstHookTestRequest.newBuilder()
            .setId("test-1")
            .setTestStatus(TEST_STATUS_PASSED)
            .build());

    // Then
    final ArgumentCaptor<AstHook> hookCaptor = ArgumentCaptor.forClass(AstHook.class);
    verify(mockHooksConfigStore).upsertObject(eq(mockRequestContext), hookCaptor.capture());
    final AstHook upsertedHook = hookCaptor.getValue();
    assertTrue(upsertedHook.hasLastTestStatus());
    assertEquals(TEST_STATUS_PASSED, upsertedHook.getLastTestStatus());
  }

  @Test
  void testUpdateHookTestWithoutStatusDoesNotUpdateParentHook() {
    // Given
    final AstHookTest existingTest =
        AstHookTest.newBuilder().setId("test-2").setTestStatus(TEST_STATUS_PENDING).build();
    when(mockTestConfigStore.getData(mockRequestContext, "test-2"))
        .thenReturn(Optional.of(existingTest));
    when(mockTestConfigStore.upsertObject(eq(mockRequestContext), any(AstHookTest.class)))
        .thenReturn(mockTestConfigObject);
    when(mockTestConfigObject.getData()).thenReturn(existingTest);

    // When
    astHookTestManager.updateHookTest(
        mockRequestContext,
        UpdateAstHookTestRequest.newBuilder().setId("test-2").setLogs("some log").build());

    // Then
    verify(mockHooksConfigStore, never()).upsertObject(any(), any(AstHook.class));
  }
}
