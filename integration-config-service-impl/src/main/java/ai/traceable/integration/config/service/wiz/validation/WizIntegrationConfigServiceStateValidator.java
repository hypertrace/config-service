package ai.traceable.integration.config.service.wiz.validation;

import ai.traceable.integration.config.service.wiz.store.WizIntegrationConfigStore;
import com.google.inject.Inject;
import io.grpc.Status;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@RequiredArgsConstructor(onConstructor_ = @Inject)
public class WizIntegrationConfigServiceStateValidator {
  private final WizIntegrationConfigStore wizIntegrationConfigStore;

  public void validateNoExistingWizIntegration(RequestContext requestContext) {
    wizIntegrationConfigStore.getAllConfigData(requestContext).stream()
        .findAny()
        .ifPresent(
            ignored -> {
              throw Status.ALREADY_EXISTS
                  .withDescription(
                      String.format(
                          "There is a pre existing WIZ integration for this tenant within context: %s",
                          requestContext))
                  .asRuntimeException();
            });
  }
}
