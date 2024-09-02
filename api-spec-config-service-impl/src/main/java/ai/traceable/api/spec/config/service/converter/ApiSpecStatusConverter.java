package ai.traceable.api.spec.config.service.converter;

import static ai.traceable.api.spec.config.service.v1.ApiSpecStatus.API_SPEC_STATUS_UPLOAD_COMPLETED;
import static ai.traceable.api.spec.config.service.v1.ApiSpecStatus.API_SPEC_STATUS_UPLOAD_IN_PROGRESS;

import ai.traceable.api.spec.config.service.v1.ApiSpecStatus;

public class ApiSpecStatusConverter {
  public ApiSpecStatus convert(ApiSpecStatus status) {
    switch (status) {
      case API_SPEC_STATUS_IN_PROGRESS:
        return API_SPEC_STATUS_UPLOAD_IN_PROGRESS;
      case API_SPEC_STATUS_COMPLETED:
        return API_SPEC_STATUS_UPLOAD_COMPLETED;
      default:
        return status;
    }
  }
}
