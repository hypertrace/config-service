package ai.traceable.ast.hooks.config.service;

import ai.traceable.ast.hooks.config.service.v1.AstHookType;
import ai.traceable.ast.hooks.config.service.v1.CreateAstHookRequest;
import ai.traceable.ast.hooks.config.service.v1.DeleteAstHookRequest;
import ai.traceable.ast.hooks.config.service.v1.UpdateAstHookRequest;
import io.grpc.Status;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@AllArgsConstructor(onConstructor_ = {@Inject})
class RequestValidator {
  void validate(CreateAstHookRequest request) {
    if (StringUtils.isBlank(request.getHookDetails().getName())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("name not found while trying to create ast hooks config")
          .asRuntimeException();
    }
    if (StringUtils.isBlank(request.getHookDetails().getCodeSnippet())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Code snippet not found while trying to create ast hooks config")
          .asRuntimeException();
    }
  }

  void validate(UpdateAstHookRequest request) {
    if (StringUtils.isBlank(request.getId())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("id expected while trying to update ast hook")
          .asRuntimeException();
    }
    if (AstHookType.UNRECOGNIZED.equals(request.getHookType())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("unrecognized ast hook type")
          .asRuntimeException();
    }
  }

  void validate(DeleteAstHookRequest request) {
    if (StringUtils.isBlank(request.getId())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Id not found while trying to delete ast hooks config")
          .asRuntimeException();
    }
  }
}
