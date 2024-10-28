package ai.traceable.config.service.rest.servlet;

import io.grpc.Status;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@javax.ws.rs.ext.Provider
@AllArgsConstructor(onConstructor_ = @Inject)
public class ExceptionMapper implements javax.ws.rs.ext.ExceptionMapper<Throwable> {

  private final Provider<RequestContext> requestContextProvider;

  @Override
  public Response toResponse(Throwable exception) {
    if (exception instanceof NotFoundException) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    RequestContext requestContext = requestContextProvider.get();
    log.warn("Got exception in context {}", requestContext, exception);

    return fromGrpcStatus(Status.fromThrowable(exception), requestContext);
  }

  private Response fromGrpcStatus(Status status, RequestContext requestContext) {
    switch (status.getCode()) {
      case INVALID_ARGUMENT:
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(Optional.ofNullable(status.getDescription()).orElse("Error"))
            .type(MediaType.TEXT_PLAIN_TYPE)
            .build();
      case PERMISSION_DENIED:
        return Response.status(Response.Status.FORBIDDEN)
            .entity(Optional.ofNullable(status.getDescription()).orElse("Error"))
            .type(MediaType.TEXT_PLAIN_TYPE)
            .build();
      case NOT_FOUND:
        return Response.status(Response.Status.NOT_FOUND).type(MediaType.TEXT_PLAIN_TYPE).build();
      default:
        return Response.serverError().entity("Error").type(MediaType.TEXT_PLAIN_TYPE).build();
    }
  }
}
