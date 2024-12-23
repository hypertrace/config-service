package ai.traceable.config.service.rest.servlet;

import static org.hypertrace.core.grpcutils.context.RequestContextConstants.HEADER_PREFIXES_TO_BE_PROPAGATED;
import static org.hypertrace.core.grpcutils.context.RequestContextConstants.REQUEST_ID_HEADER_KEY;

import com.google.common.base.Suppliers;
import com.google.common.collect.Streams;
import com.google.inject.servlet.RequestScoped;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.servlet.http.HttpServletRequest;
import java.util.UUID;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@RequestScoped
@AllArgsConstructor(onConstructor_ = @Inject)
class ContextProvider implements Provider<RequestContext> {
  private final HttpServletRequest request;

  // Memoize to avoid creating a different context for two different provider calls
  private final Supplier<RequestContext> memoizedSupplier = Suppliers.memoize(this::build);

  @Override
  public RequestContext get() {
    return memoizedSupplier.get();
  }

  private RequestContext build() {
    RequestContext requestContext = new RequestContext();
    requestContext.put(REQUEST_ID_HEADER_KEY, UUID.randomUUID().toString());

    Streams.stream(request.getHeaderNames().asIterator())
        .map(String::toLowerCase)
        .filter(
            headerName ->
                HEADER_PREFIXES_TO_BE_PROPAGATED.stream()
                    .map(String::toLowerCase)
                    .anyMatch(headerName::startsWith))
        .forEach(
            propagableHeader ->
                requestContext.put(propagableHeader, request.getHeader(propagableHeader)));
    return requestContext;
  }
}
