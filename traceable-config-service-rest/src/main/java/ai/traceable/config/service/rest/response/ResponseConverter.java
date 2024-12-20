package ai.traceable.config.service.rest.response;

import ai.traceable.config.service.rest.v1.HttpResponseMetadata;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import jakarta.inject.Inject;
import java.util.function.Function;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class ResponseConverter {
  private final RedirectResponseConverter redirectResponseConverter;
  private final SimpleResponseConverter simpleResponseConverter;
  private final CookiesBuilder cookiesBuilder;
  private final HeadersBuilder headersBuilder;

  public Response convert(Object responseBody, HttpResponseMetadata metadata) {
    return this.decorateHeaders(metadata, this.buildPartialResponse(responseBody, metadata));
  }

  public Response convertMetadata(HttpResponseMetadata metadata) {
    return this.decorateHeaders(metadata, this.buildPartialResponse(metadata));
  }

  public Response convertMetadata(
      HttpServletResponse existingResponse, HttpResponseMetadata metadata) {
    return this.decorateHeaders(
        existingResponse, this.decorateHeaders(metadata, this.buildPartialResponse(metadata)));
  }

  // Inserts/replaces the given body in the response
  public Response convertBody(HttpServletResponse existingResponse, Object body) {
    return this.decorateHeaders(existingResponse, Response.ok(body).build());
  }

  private Response buildPartialResponse(HttpResponseMetadata responseMetadata) {
    switch (responseMetadata.getStatusCase()) {
      case REDIRECT_RESPONSE:
        return this.redirectResponseConverter.convert(responseMetadata);
      case OK_RESPONSE:
      case UNAUTHENTICATED:
      default:
        return this.simpleResponseConverter.convertMetadata(responseMetadata);
    }
  }

  private Response buildPartialResponse(
      Object responseBody, HttpResponseMetadata responseMetadata) {
    switch (responseMetadata.getStatusCase()) {
      case REDIRECT_RESPONSE:
        return this.redirectResponseConverter.convert(responseMetadata);
      case OK_RESPONSE:
      case UNAUTHENTICATED:
      default:
        return this.simpleResponseConverter.convert(responseBody, responseMetadata);
    }
  }

  public Response convert(Object responseBody) {
    // assuming no-metadata means 200 OK
    return this.simpleResponseConverter.convert(responseBody);
  }

  private Response decorateHeaders(HttpResponseMetadata metadata, Response response) {
    Response.ResponseBuilder responseBuilder = Response.fromResponse(response);
    this.cookiesBuilder.addCookies(responseBuilder, metadata.getCookiesList());
    this.headersBuilder.addHeaders(responseBuilder, metadata.getHeadersMap());
    return responseBuilder.build();
  }

  private Response decorateHeaders(HttpServletResponse servletResponse, Response response) {
    Response.ResponseBuilder responseBuilder = Response.fromResponse(response);
    Multimap<String, String> existingHeaders =
        servletResponse.getHeaderNames().stream()
            .collect(
                ImmutableListMultimap.flatteningToImmutableListMultimap(
                    Function.identity(),
                    headerName -> servletResponse.getHeaders(headerName).stream()));
    this.headersBuilder.addHeaders(responseBuilder, existingHeaders);
    return responseBuilder.build();
  }
}
