package ai.traceable.config.service.rest.response;

import static ai.traceable.config.service.rest.v1.HttpResponseMetadata.StatusCase.OK_RESPONSE;
import static ai.traceable.config.service.rest.v1.HttpResponseMetadata.StatusCase.UNAUTHENTICATED;

import ai.traceable.config.service.rest.v1.HttpResponseMetadata;
import com.google.common.collect.ImmutableMap;
import jakarta.inject.Inject;
import java.util.Map;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
class SimpleResponseConverter {

  private static final Map<HttpResponseMetadata.StatusCase, Status> responseEnumToCodeMap =
      ImmutableMap.<HttpResponseMetadata.StatusCase, Status>builder()
          .put(OK_RESPONSE, Status.OK)
          .put(UNAUTHENTICATED, Status.UNAUTHORIZED)
          .build();

  /**
   * This will construct a generic response from the metadata, message. The response would have the
   * statusCode, message body, headers. The response will not have response specific headers
   *
   * @param responseBody
   * @param metadata
   * @return
   */
  public Response convert(Object responseBody, HttpResponseMetadata metadata) {
    return Response.status(responseEnumToCodeMap.getOrDefault(metadata.getStatusCase(), Status.OK))
        .entity(responseBody)
        .build();
  }

  public Response convert(Object responseBody) {
    return Response.ok(responseBody).build();
  }

  public Response convertMetadata(HttpResponseMetadata metadata) {
    return Response.status(responseEnumToCodeMap.getOrDefault(metadata.getStatusCase(), Status.OK))
        .build();
  }
}
