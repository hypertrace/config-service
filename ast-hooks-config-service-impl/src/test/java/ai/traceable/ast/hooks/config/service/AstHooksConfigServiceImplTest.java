package ai.traceable.ast.hooks.config.service;

import static ai.traceable.ast.hooks.config.service.v1.AuthMechanism.AUTH_MECHANISM_AUTH0_CLIENT_CREDENTIALS;
import static ai.traceable.ast.hooks.config.service.v1.Role.ROLE_ADMIN;
import static ai.traceable.ast.hooks.config.service.v1.TestStatus.TEST_STATUS_ABORTED;
import static ai.traceable.ast.hooks.config.service.v1.TestStatus.TEST_STATUS_PENDING;
import static ai.traceable.ast.hooks.config.service.v1.TestStatus.TEST_STATUS_RUNNER_ASSIGNED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.ast.hooks.config.service.handlers.AstHookTestManager;
import ai.traceable.ast.hooks.config.service.handlers.CreateAstHookHandler;
import ai.traceable.ast.hooks.config.service.handlers.UpdateAstHookConfigHandler;
import ai.traceable.ast.hooks.config.service.handlers.UpdateAstHookHandler;
import ai.traceable.ast.hooks.config.service.store.AstHooksConfigStore;
import ai.traceable.ast.hooks.config.service.store.AstHooksTestConfigStore;
import ai.traceable.ast.hooks.config.service.v1.AdvancedMode;
import ai.traceable.ast.hooks.config.service.v1.AllowedRunners;
import ai.traceable.ast.hooks.config.service.v1.AllowedRunnersInfo;
import ai.traceable.ast.hooks.config.service.v1.AstHookTest;
import ai.traceable.ast.hooks.config.service.v1.AstHookTestDetails;
import ai.traceable.ast.hooks.config.service.v1.AstHookTestFilter;
import ai.traceable.ast.hooks.config.service.v1.AstHookTestResult;
import ai.traceable.ast.hooks.config.service.v1.AstHooksConfigServiceGrpc;
import ai.traceable.ast.hooks.config.service.v1.CreateAstHookTestRequest;
import ai.traceable.ast.hooks.config.service.v1.DeleteAstHookTestsRequest;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookTestResultRequest;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookTestResultResponse;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookTestsRequest;
import ai.traceable.ast.hooks.config.service.v1.RunnerInfo;
import ai.traceable.ast.hooks.config.service.v1.TestStatus;
import ai.traceable.ast.hooks.config.service.v1.TestStatusFilter;
import ai.traceable.ast.hooks.config.service.v1.UpdateAstHookTestRequest;
import ai.traceable.ast.hooks.config.service.validators.RequestValidator;
import ai.traceable.config.utils.UuidGenerator;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AstHooksConfigServiceImplTest {
  MockGenericConfigService mockGenericConfigService;
  @Mock ConfigChangeEventGenerator mockConfigChangeEventGenerator;
  @Mock RequestValidator mockValidator;
  @Mock CreateAstHookHandler mockCreateAstHookHandler;
  @Mock UpdateAstHookHandler mockUpdateAstHookHandler;
  @Mock UuidGenerator mockUuidGenerator;
  @Mock UpdateAstHookConfigHandler mockUpdateAstHookConfigHandler;

  AstHooksConfigServiceGrpc.AstHooksConfigServiceBlockingStub stub;

  @BeforeEach
  void setUp() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDeleteAll();
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    final AstHooksTestConfigStore astHooksTestConfigStore =
        new AstHooksTestConfigStore(configServiceBlockingStub, mockConfigChangeEventGenerator);
    mockGenericConfigService
        .addService(
            new AstHooksConfigServiceImpl(
                mockValidator,
                mockCreateAstHookHandler,
                mockUpdateAstHookHandler,
                new AstHooksConfigStore(configServiceBlockingStub, mockConfigChangeEventGenerator),
                astHooksTestConfigStore,
                new AstHookTestManager(
                    mockUuidGenerator, astHooksTestConfigStore, mockUpdateAstHookConfigHandler)))
        .start();
    stub = AstHooksConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
  }

  @Test
  void testCrud() {
    AstHookTestDetails astHookTestDetails =
        AstHookTestDetails.newBuilder()
            .setRole(ROLE_ADMIN)
            .setAdvancedMode(
                AdvancedMode.newBuilder()
                    .setAuthMechanism(AUTH_MECHANISM_AUTH0_CLIENT_CREDENTIALS)
                    .setCodeSnippet("codeSnippet")
                    .build())
            .build();
    AstHookTest astHookTest1 =
        AstHookTest.newBuilder()
            .setId("id1")
            .setTestStatus(TEST_STATUS_PENDING)
            .setAllowedRunners(AllowedRunners.newBuilder().setAnyRunner(true).build())
            .setAstHookTestDetails(astHookTestDetails)
            .build();
    AstHookTest updatedAstHookTest1 =
        AstHookTest.newBuilder()
            .setId("id1")
            .setTestStatus(TEST_STATUS_RUNNER_ASSIGNED)
            .setAllowedRunners(AllowedRunners.newBuilder().setAnyRunner(true).build())
            .setAstHookTestDetails(astHookTestDetails)
            .build();
    AstHookTest astHookTest2 =
        AstHookTest.newBuilder()
            .setId("id2")
            .setTestStatus(TEST_STATUS_PENDING)
            .setAllowedRunners(
                AllowedRunners.newBuilder()
                    .setRunnersInfo(
                        AllowedRunnersInfo.newBuilder()
                            .addRunnersInfo(RunnerInfo.newBuilder().setId("runner1").build())
                            .build())
                    .build())
            .setAstHookTestDetails(astHookTestDetails)
            .build();
    when(mockUuidGenerator.generateRandomId()).thenReturn("id1");
    AstHookTest astHookTestFirstCreated =
        stub.createAstHookTest(
                CreateAstHookTestRequest.newBuilder()
                    .setHookTestDetails(astHookTestDetails)
                    .setAllowedRunners(AllowedRunners.newBuilder().setAnyRunner(true).build())
                    .build())
            .getAstHookTest();
    assertEquals(astHookTest1, astHookTestFirstCreated);
    assertEquals(
        GetAstHookTestResultResponse.newBuilder()
            .setHookTestResult(
                AstHookTestResult.newBuilder()
                    .setTestStatus(TEST_STATUS_PENDING)
                    .setId("id1")
                    .build())
            .build(),
        stub.getAstHookTestResult(GetAstHookTestResultRequest.newBuilder().setId("id1").build()));

    when(mockUuidGenerator.generateRandomId()).thenReturn("id2");
    AstHookTest astHookTestSecondCreated =
        stub.createAstHookTest(
                CreateAstHookTestRequest.newBuilder()
                    .setHookTestDetails(astHookTestDetails)
                    .setAllowedRunners(
                        AllowedRunners.newBuilder()
                            .setRunnersInfo(
                                AllowedRunnersInfo.newBuilder()
                                    .addRunnersInfo(
                                        RunnerInfo.newBuilder().setId("runner1").build())
                                    .build())
                            .build())
                    .build())
            .getAstHookTest();
    assertEquals(astHookTest2, astHookTestSecondCreated);
    assertEquals(
        GetAstHookTestResultResponse.newBuilder()
            .setHookTestResult(
                AstHookTestResult.newBuilder()
                    .setTestStatus(TEST_STATUS_PENDING)
                    .setId("id2")
                    .build())
            .build(),
        stub.getAstHookTestResult(GetAstHookTestResultRequest.newBuilder().setId("id2").build()));

    assertEquals(
        List.of(astHookTest2, astHookTest1),
        stub.getAstHookTests(
                GetAstHookTestsRequest.newBuilder()
                    .addAstHookTestFilters(buildTestStatusFilter(TEST_STATUS_PENDING))
                    .build())
            .getAstHookTestsList());
    assertTrue(
        stub.getAstHookTests(
                GetAstHookTestsRequest.newBuilder()
                    .addAstHookTestFilters(buildTestStatusFilter(TEST_STATUS_ABORTED))
                    .build())
            .getAstHookTestsList()
            .isEmpty());

    AstHookTest firstAstHookTestUpdated =
        stub.updateAstHookTest(
                UpdateAstHookTestRequest.newBuilder()
                    .setId("id1")
                    .setTestStatus(TEST_STATUS_RUNNER_ASSIGNED)
                    .build())
            .getAstHookTest();
    assertEquals(updatedAstHookTest1, firstAstHookTestUpdated);
    assertEquals(
        List.of(astHookTest2),
        stub.getAstHookTests(
                GetAstHookTestsRequest.newBuilder()
                    .addAstHookTestFilters(buildTestStatusFilter(TEST_STATUS_PENDING))
                    .build())
            .getAstHookTestsList());
    assertEquals(
        List.of(updatedAstHookTest1),
        stub.getAstHookTests(
                GetAstHookTestsRequest.newBuilder()
                    .addAstHookTestFilters(buildTestStatusFilter(TEST_STATUS_RUNNER_ASSIGNED))
                    .build())
            .getAstHookTestsList());

    stub.deleteAstHookTests(DeleteAstHookTestsRequest.newBuilder().addIds("id1").build());

    assertEquals(
        List.of(astHookTest2),
        stub.getAstHookTests(GetAstHookTestsRequest.getDefaultInstance()).getAstHookTestsList());
  }

  private static AstHookTestFilter buildTestStatusFilter(TestStatus testStatus) {
    return AstHookTestFilter.newBuilder()
        .setTestStatusFilter(TestStatusFilter.newBuilder().addStatuses(testStatus).build())
        .build();
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }
}
