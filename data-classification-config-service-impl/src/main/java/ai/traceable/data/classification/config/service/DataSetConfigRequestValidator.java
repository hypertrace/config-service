package ai.traceable.data.classification.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.data.classification.config.service.v1.CreateDataSetRequest;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DeleteDataSetRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetsRequest;
import ai.traceable.data.classification.config.service.v1.UpdateDataSetRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DataSetConfigRequestValidator {
  public void validateOrThrow(RequestContext requestContext, CreateDataSetRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateDataSetInfo(request.getInfo());
  }

  public void validateOrThrow(RequestContext requestContext, GetDataSetRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, GetDataSetRequest.ID_FIELD_NUMBER);
  }

  public void validateOrThrow(RequestContext requestContext, GetDataSetsRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(RequestContext requestContext, UpdateDataSetRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateDataSetRequest.ID_FIELD_NUMBER);
    validateDataSetInfo(request.getInfo());
  }

  public void validateOrThrow(RequestContext requestContext, DeleteDataSetRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteDataSetRequest.ID_FIELD_NUMBER);
  }

  private void validateDataSetInfo(DataSetInfo info) {
    validateNonDefaultPresenceOrThrow(info, DataSetInfo.NAME_FIELD_NUMBER);
    if (info.getDataTypeIdsCount() > 0) {
      validateNonDefaultPresenceOrThrow(info, DataSetInfo.DATA_TYPE_IDS_FIELD_NUMBER);
    }
  }
}
