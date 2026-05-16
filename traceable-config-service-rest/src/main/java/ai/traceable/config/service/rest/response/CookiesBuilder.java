package ai.traceable.config.service.rest.response;

import ai.traceable.config.service.rest.v1.Cookie;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response.ResponseBuilder;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.http.HttpCookie;

@Slf4j
class CookiesBuilder {

  public ResponseBuilder addCookies(ResponseBuilder builder, List<Cookie> cookies) {
    cookies.forEach(
        cookie -> builder.header(HttpHeaders.SET_COOKIE, this.buildSetCookieString(cookie)));
    return builder;
  }

  private String buildSetCookieString(Cookie cookie) {
    // We use jetty impl directly to build the header string so we're not coupled to any specific
    // impl or version. This allows us to accomplish a few things:
    // 1. unify our impl between jax-rs and the container (happens to be jetty, but not required)
    // 2. still remain decoupled from the container impl (since we're going back to a string)
    // 3. use newer cookie attributes that neither spec supports like same-site

    HttpCookie.Builder builder =
        HttpCookie.build(cookie.getKey(), cookie.getValue())
            .secure(true)
            .httpOnly(cookie.getHttpOnly());
    if (cookie.hasMaxAge()) {
      builder.maxAge(cookie.getMaxAge().getSeconds());
    }
    this.convertSameSiteToJetty(cookie.getSameSite()).ifPresent(builder::sameSite);
    return builder.build().toString();
  }

  private Optional<HttpCookie.SameSite> convertSameSiteToJetty(Cookie.SameSite sameSite) {
    switch (sameSite) {
      case SAME_SITE_LAX:
        return Optional.of(HttpCookie.SameSite.LAX);
      case SAME_SITE_STRICT:
        return Optional.of(HttpCookie.SameSite.STRICT);
      case SAME_SITE_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        return Optional.empty();
    }
  }
}
