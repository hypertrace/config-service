package ai.traceable.ast.hooks.config.service.validators;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.ast.hooks.config.service.v1.AllowedRunners;
import ai.traceable.ast.hooks.config.service.v1.AstHookDetails;
import ai.traceable.ast.hooks.config.service.v1.AstHookTestDetails;
import ai.traceable.ast.hooks.config.service.v1.AstHookTestFilter;
import ai.traceable.ast.hooks.config.service.v1.AstHookType;
import ai.traceable.ast.hooks.config.service.v1.CreateAstHookRequest;
import ai.traceable.ast.hooks.config.service.v1.CreateAstHookTestRequest;
import ai.traceable.ast.hooks.config.service.v1.DeleteAstHookRequest;
import ai.traceable.ast.hooks.config.service.v1.DeleteAstHookTestsRequest;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookTestResultRequest;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookTestsRequest;
import ai.traceable.ast.hooks.config.service.v1.HookConfig;
import ai.traceable.ast.hooks.config.service.v1.Role;
import ai.traceable.ast.hooks.config.service.v1.TestStatus;
import ai.traceable.ast.hooks.config.service.v1.TestStatusFilter;
import ai.traceable.ast.hooks.config.service.v1.UpdateAstHookRequest;
import ai.traceable.ast.hooks.config.service.v1.UpdateAstHookTestRequest;
import io.grpc.Status;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RequestValidator extends ValidatorBase {

  private final HookConfigValidator hookConfigValidator;

  public void validate(RequestContext requestContext, CreateAstHookRequest request) {
    if (isBlank(request.getHookDetails().getName())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("name not found while trying to create ast hooks config")
          .asRuntimeException(requestContext.buildTrailers());
    }
    if (isBlank(request.getHookDetails().getCodeSnippet())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Code snippet not found while trying to create ast hooks config")
          .asRuntimeException(requestContext.buildTrailers());
    }
    if (request.getHookDetails().hasAdvancedMode()
        && isBlank(request.getHookDetails().getAdvancedMode().getCodeSnippet())) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Code snippet not found while trying to create ast hooks config in advanced mode")
          .asRuntimeException(requestContext.buildTrailers());
    }
    validateHookDetails(requestContext, request.getHookDetails());
  }

  public void validate(RequestContext requestContext, UpdateAstHookRequest request) {

    validateStringNotBlank(request.getId(), "id expected while trying to update ast hook");
    if (AstHookType.UNRECOGNIZED.equals(request.getHookType())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("unrecognized ast hook type")
          .asRuntimeException(requestContext.buildTrailers());
    }
    if (request.hasAstHookDetails()) {
      validateHookDetails(requestContext, request.getAstHookDetails());
    }
  }

  public void validate(RequestContext requestContext, DeleteAstHookRequest request) {
    if (isBlank(request.getId())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Id not found while trying to delete ast hooks config")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateHookDetails(RequestContext requestContext, AstHookDetails astHookDetails) {
    switch (astHookDetails.getAstHookTypeCase()) {
      case HOOK_CONFIG:
        if (!astHookDetails.hasHookConfig()) {
          throw Status.INVALID_ARGUMENT
              .withDescription("hook config expected in normal mode")
              .asRuntimeException(requestContext.buildTrailers());
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

  private void validateRole(RequestContext requestContext, Role role) {
    switch (role) {
      case ROLE_ADMIN:
      case ROLE_READER:
      case ROLE_WRITER:
      case ROLE_STANDARD:
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unknown role, got " + role.name())
            .asRuntimeException(requestContext.buildTrailers());
    }
  }

  public void validateOrThrow(RequestContext requestContext, GetAstHookTestResultRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, GetAstHookTestResultRequest.ID_FIELD_NUMBER);
  }

  public void validateOrThrow(RequestContext requestContext, DeleteAstHookTestsRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request.getIdsCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Ids not found while trying to delete ast hook test configs")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  public void validateOrThrow(RequestContext requestContext, GetAstHookTestsRequest request) {
    validateRequestContextOrThrow(requestContext);
    request
        .getAstHookTestFiltersList()
        .forEach(astHookTestFilter -> validateAstHookFilter(requestContext, astHookTestFilter));
  }

  private void validateAstHookFilter(
      RequestContext requestContext, AstHookTestFilter astHookTestFilter) {
    switch (astHookTestFilter.getFilterCase()) {
      case TEST_STATUS_FILTER:
        validateTestStatusFilter(requestContext, astHookTestFilter.getTestStatusFilter());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Encountered unknown ast hook test filer type: {}" + astHookTestFilter)
            .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateTestStatusFilter(
      RequestContext requestContext, TestStatusFilter testStatusFilter) {
    if (testStatusFilter.getStatusesCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Statuses not found within test status filter: " + testStatusFilter)
          .asRuntimeException(requestContext.buildTrailers());
    }
    testStatusFilter
        .getStatusesList()
        .forEach(status -> this.validateTestStatus(requestContext, status));
  }

  public void validateOrThrow(RequestContext requestContext, CreateAstHookTestRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateHookTestDetails(requestContext, request.getHookTestDetails());
    validateAllowedRunners(requestContext, request.getAllowedRunners());
  }

  private void validateHookTestDetails(
      RequestContext requestContext, AstHookTestDetails hookTestDetails) {
    switch (hookTestDetails.getAstHookTypeCase()) {
      case HOOK_CONFIG:
        if (!hookTestDetails.hasHookConfig()) {
          throw Status.INVALID_ARGUMENT
              .withDescription("hook config expected in normal mode")
              .asRuntimeException(requestContext.buildTrailers());
        }
        validateHookConfig(hookTestDetails.getHookConfig());
      case ADVANCED_MODE:
        validateStringNotBlank(
            hookTestDetails.getAdvancedMode().getCodeSnippet(),
            " Code snippet not found while trying to create ast hooks test config");
    }
    validateRole(requestContext, hookTestDetails.getRole());
  }

  public void validateOrThrow(RequestContext requestContext, UpdateAstHookTestRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateAstHookTestRequest.ID_FIELD_NUMBER);

    if (request.hasTestStatus()) {
      validateTestStatus(requestContext, request.getTestStatus());
    }
    if (request.hasLogs()) {
      validateLogs(requestContext, request.getLogs());
    }
  }

  private void validateAllowedRunners(
      RequestContext requestContext, AllowedRunners allowedRunners) {
    if (AllowedRunners.getDefaultInstance().equals(allowedRunners)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Allowed runners data not found: " + allowedRunners)
          .asRuntimeException(requestContext.buildTrailers());
    }
    if (allowedRunners.hasRunnersInfo()
        && allowedRunners.getRunnersInfo().getRunnersInfoCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Allowed runners info list is empty: " + allowedRunners.getRunnersInfo())
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateTestStatus(RequestContext requestContext, TestStatus testStatus) {
    switch (testStatus) {
      case TEST_STATUS_FAILED:
      case TEST_STATUS_PASSED:
      case TEST_STATUS_ABORTED:
      case TEST_STATUS_PENDING:
      case TEST_STATUS_RUNNER_ASSIGNED:
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Encountered unknown test status: " + testStatus)
            .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private static void validateLogs(RequestContext requestContext, String logs) {
    if (isBlank(logs)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Empty Log while trying to update ast hooks test config")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }
}
