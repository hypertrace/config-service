package ai.traceable.ast.hooks.config.service.handlers;

import static ai.traceable.ast.hooks.config.service.v1.TestStatus.TEST_STATUS_PENDING;

import ai.traceable.ast.hooks.config.service.store.AstHooksTestConfigStore;
import ai.traceable.ast.hooks.config.service.v1.AstHookTest;
import ai.traceable.ast.hooks.config.service.v1.AstHookTestDetails;
import ai.traceable.ast.hooks.config.service.v1.CreateAstHookTestRequest;
import ai.traceable.ast.hooks.config.service.v1.HookConfig;
import ai.traceable.ast.hooks.config.service.v1.UpdateAstHookTestRequest;
import ai.traceable.config.utils.UuidGenerator;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class AstHookTestManager {

  private final UuidGenerator uuidGenerator;
  private final AstHooksTestConfigStore configStore;
  private final UpdateAstHookConfigHandler updateAstHookConfigHandler;

  public AstHookTest createHookTest(
      final RequestContext requestContext,
      final CreateAstHookTestRequest request,
      final HookConfig oldHookConfig) {
    String id = uuidGenerator.generateRandomId();
    AstHookTestDetails astHookTestDetails =
        request.hasAstHookId()
            ? resolveHookTestDetails(request.getHookTestDetails(), oldHookConfig)
            : request.getHookTestDetails();
    AstHookTest astHookTest =
        AstHookTest.newBuilder()
            .setId(id)
            .setAstHookTestDetails(astHookTestDetails)
            .setTestStatus(TEST_STATUS_PENDING)
            .setAllowedRunners(request.getAllowedRunners())
            .build();
    return configStore.upsertObject(requestContext, astHookTest).getData();
  }

  public AstHookTest updateHookTest(
      final RequestContext requestContext, final UpdateAstHookTestRequest request) {
    AstHookTest oldHookTest =
        configStore
            .getData(requestContext, request.getId())
            .orElseThrow(
                () -> new IllegalArgumentException("Trying to update non existent ast hook test"));

    AstHookTest.Builder updatedHookTestBuilder = oldHookTest.toBuilder();
    applyTestStatusUpdate(request, updatedHookTestBuilder);
    applyLogsUpdate(request, updatedHookTestBuilder);

    return configStore.upsertObject(requestContext, updatedHookTestBuilder.build()).getData();
  }

  private void applyLogsUpdate(
      UpdateAstHookTestRequest request, AstHookTest.Builder updatedHookTestBuilder) {
    if (request.hasLogs()) {
      updatedHookTestBuilder.addLogs(request.getLogs());
    }
  }

  private void applyTestStatusUpdate(
      UpdateAstHookTestRequest request, AstHookTest.Builder updatedHookTestBuilder) {
    if (request.hasTestStatus()) {
      updatedHookTestBuilder.setTestStatus(request.getTestStatus());
    }
  }

  private AstHookTestDetails resolveHookTestDetails(
      AstHookTestDetails hookTestDetails, HookConfig oldHookConfig) {
    AstHookTestDetails.Builder astHookTestDetailsBuilder =
        AstHookTestDetails.newBuilder().setRole(hookTestDetails.getRole());
    if (hookTestDetails.hasHookConfig()) {
      astHookTestDetailsBuilder.setHookConfig(
          updateAstHookConfigHandler.applyHookConfigUpdate(
              hookTestDetails.getHookConfig(), oldHookConfig));
    } else if (hookTestDetails.hasAdvancedMode()) {
      astHookTestDetailsBuilder.setAdvancedMode(hookTestDetails.getAdvancedMode());
    }
    return astHookTestDetailsBuilder.build();
  }
}
