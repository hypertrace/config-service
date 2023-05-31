package ai.traceable.ast.hooks.config.service.validators;

import ai.traceable.ast.hooks.config.service.v1.AstHookDetails;
import ai.traceable.ast.hooks.config.service.v1.AstHookType;
import ai.traceable.ast.hooks.config.service.v1.CreateAstHookRequest;
import ai.traceable.ast.hooks.config.service.v1.DeleteAstHookRequest;
import ai.traceable.ast.hooks.config.service.v1.HookConfig;
import ai.traceable.ast.hooks.config.service.v1.Role;
import ai.traceable.ast.hooks.config.service.v1.UpdateAstHookRequest;
import io.grpc.Status;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RequestValidator extends ValidatorBase {

  private final HookConfigValidator hookConfigValidator;

  public void validate(CreateAstHookRequest request) {
    if (StringUtils.isBlank(request.getHookDetails().getName())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("name not found while trying to create ast hooks config")
          .asRuntimeException(getRequestContext().buildTrailers());
    }
    if (StringUtils.isBlank(request.getHookDetails().getCodeSnippet())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Code snippet not found while trying to create ast hooks config")
          .asRuntimeException(getRequestContext().buildTrailers());
    }
    if (request.getHookDetails().hasAdvancedMode()
        && StringUtils.isBlank(request.getHookDetails().getAdvancedMode().getCodeSnippet())) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Code snippet not found while trying to create ast hooks config in advanced mode")
          .asRuntimeException(getRequestContext().buildTrailers());
    }
    validateHookDetails(request.getHookDetails());
  }

  public void validate(UpdateAstHookRequest request) {

    validateStringNotBlank(request.getId(), "id expected while trying to update ast hook");
    if (AstHookType.UNRECOGNIZED.equals(request.getHookType())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("unrecognized ast hook type")
          .asRuntimeException(getRequestContext().buildTrailers());
    }
    if (request.hasAstHookDetails()) {
      validateHookDetails(request.getAstHookDetails());
    }
  }

  public void validate(DeleteAstHookRequest request) {
    if (StringUtils.isBlank(request.getId())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Id not found while trying to delete ast hooks config")
          .asRuntimeException(getRequestContext().buildTrailers());
    }
  }

  private void validateHookDetails(AstHookDetails astHookDetails) {
    switch (astHookDetails.getAstHookTypeCase()) {
      case HOOK_CONFIG:
        if (!astHookDetails.hasHookConfig()) {
          throw Status.INVALID_ARGUMENT
              .withDescription("hook config expected in normal mode")
              .asRuntimeException(getRequestContext().buildTrailers());
        }
        validateHookConfig(astHookDetails.getHookConfig());
      case ADVANCED_MODE:
        validateStringNotBlank(
            astHookDetails.getAdvancedMode().getCodeSnippet(),
            " Code snippet not found while trying to create ast hooks config");
    }
    validateStringNotBlank(
        astHookDetails.getName(), "name not found while trying to create ast hooks config");
    //    validateRole(astHookDetails.getRole());
  }

  private void validateHookConfig(HookConfig hookConfig) {
    hookConfigValidator.validate(hookConfig);
  }

  private void validateRole(Role role) {
    switch (role) {
      case ROLE_ADMIN:
      case ROLE_READER:
      case ROLE_WRITER:
      case ROLE_STANDARD:
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unknown role, got " + role.name())
            .asRuntimeException(getRequestContext().buildTrailers());
    }
  }
}
