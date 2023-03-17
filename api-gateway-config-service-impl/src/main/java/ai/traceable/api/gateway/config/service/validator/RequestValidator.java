package ai.traceable.api.gateway.config.service.validator;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.api.gateway.config.service.v1.CreateRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.DeleteRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.GetRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.Metadata;
import ai.traceable.api.gateway.config.service.v1.OrgIds;
import ai.traceable.api.gateway.config.service.v1.RouteInfo;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RequestValidator {
  public void validate(
      @SuppressWarnings("unused") final CreateRoutesRequest request,
      final RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    request
        .getRoutesList()
        .forEach(
            route ->
                validateNonDefaultPresenceOrThrow(route.getInfo(), RouteInfo.PATH_FIELD_NUMBER));
    request
        .getRoutesList()
        .forEach(
            route ->
                validateNonDefaultPresenceOrThrow(
                    route.getMetadata(), Metadata.ORG_ID_FIELD_NUMBER));
  }

  public void validate(
      @SuppressWarnings("unused") final GetRoutesRequest request,
      final RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validate(final DeleteRoutesRequest request, final RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request.getFilter().getOrgIds(), OrgIds.ORG_ID_FIELD_NUMBER);
  }
}
