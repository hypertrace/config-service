package ai.traceable.config.service.rest.response;

import ai.traceable.config.service.rest.v1.HttpResponseMetadata;
import ai.traceable.config.service.rest.v1.RedirectResponse;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
class RedirectResponseConverter {

  public Response convert(HttpResponseMetadata metadata) {
    switch (metadata.getRedirectResponse().getCode()) {
      case REDIRECT_STATUS_CODE_OK_HTML_REDIRECT:
        return buildHtmlRedirect(metadata.getRedirectResponse());
      case REDIRECT_STATUS_CODE_TEMPORARY_REDIRECT:
      default:
        return this.buildTemporaryRedirect(metadata.getRedirectResponse());
    }
  }

  private Response buildTemporaryRedirect(RedirectResponse response) {
    return Response.temporaryRedirect(URI.create(response.getRedirectUrl())).build();
  }

  private Response buildHtmlRedirect(RedirectResponse response) {
    return Response.ok(
            String.format(
                "<meta http-equiv=\"refresh\" content=\"0; url='%s'\" />",
                response.getRedirectUrl()),
            MediaType.TEXT_HTML_TYPE)
        .build();
  }
}
