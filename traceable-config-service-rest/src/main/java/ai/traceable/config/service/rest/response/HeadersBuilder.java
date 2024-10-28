package ai.traceable.config.service.rest.response;

import com.google.common.collect.Multimap;
import java.util.Map;
import javax.ws.rs.core.Response.ResponseBuilder;

class HeadersBuilder {
  public ResponseBuilder addHeaders(ResponseBuilder builder, Map<String, String> headers) {
    headers.forEach(builder::header);
    return builder;
  }

  public ResponseBuilder addHeaders(ResponseBuilder response, Multimap<String, String> headers) {
    headers.forEach(response::header);
    return response;
  }
}
