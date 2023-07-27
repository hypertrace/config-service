package ai.traceable.ast.hooks.config.service.handlers;

import ai.traceable.ast.hooks.config.service.store.AstHooksConfigStore;
import ai.traceable.ast.hooks.config.service.v1.AstHook;
import ai.traceable.ast.hooks.config.service.v1.AstHookDetails;
import ai.traceable.ast.hooks.config.service.v1.UpdateAstHookRequest;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class UpdateAstHookHandler {
  private final AstHooksConfigStore configStore;
  private final UpdateAstHookConfigHandler updateAstHookConfigHandler =
      new UpdateAstHookConfigHandler();

  public AstHook updateHook(UpdateAstHookRequest request, RequestContext requestContext) {
    AstHook oldHook =
        configStore
            .getData(requestContext, request.getId())
            .orElseThrow(
                () -> new IllegalArgumentException("Trying to update non existent ast hook"));

    AstHookDetails.Builder updatedHookDetailsBuilder = oldHook.getHookDetails().toBuilder();
    applyNameUpdate(request, updatedHookDetailsBuilder);
    applyDescriptionUpdate(request, updatedHookDetailsBuilder);
    applyCodeSnippetUpdate(request, updatedHookDetailsBuilder);
    if (request.hasAstHookDetails()) {
      updatedHookDetailsBuilder =
          applyHookDetailsUpdate(request.getAstHookDetails(), oldHook.getHookDetails());
    }
    AstHook.Builder updatedHookBuilder =
        oldHook.toBuilder().setHookDetails(updatedHookDetailsBuilder.build());
    if (request.hasHookTestId()) {
      updatedHookBuilder.setAstHookTestId(request.getHookTestId());
    }
    return configStore.upsertObject(requestContext, updatedHookBuilder.build()).getData();
  }

  private AstHookDetails.Builder applyHookDetailsUpdate(
      AstHookDetails newHookDetails, AstHookDetails oldHookDetails) {
    AstHookDetails.Builder astHookDetailsBuilder =
        AstHookDetails.newBuilder()
            .setName(newHookDetails.getName())
            .setDescription(newHookDetails.getDescription());
    if (newHookDetails.hasHookConfig()) {
      astHookDetailsBuilder.setHookConfig(
          updateAstHookConfigHandler.applyHookConfigUpdate(
              newHookDetails.getHookConfig(), oldHookDetails.getHookConfig()));
    } else if (newHookDetails.hasAdvancedMode()) {
      astHookDetailsBuilder.setAdvancedMode(newHookDetails.getAdvancedMode());
    }
    return astHookDetailsBuilder;
  }

  private void applyNameUpdate(UpdateAstHookRequest request, AstHookDetails.Builder builder) {
    if (request.hasName()) {
      builder.setName(request.getName());
    }
  }

  private void applyDescriptionUpdate(
      UpdateAstHookRequest request, AstHookDetails.Builder builder) {
    if (request.hasDescription()) {
      builder.setDescription(request.getDescription());
    }
  }

  private void applyCodeSnippetUpdate(
      UpdateAstHookRequest request, AstHookDetails.Builder builder) {
    if (request.hasCodeSnippet()) {
      builder.setCodeSnippet(request.getCodeSnippet());
    }
  }
}
