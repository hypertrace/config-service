package ai.traceable.localprocessing.config.service.spanprocessingrules;

import java.util.List;

public interface SpanAttributeConstants {
  List<String> URL_SPAN_ATTRIBUTE_KEYS =
      List.of("http.url", "http.target", "http.path", "url.full", "url.path");
  List<String> URL_PATH_SPAN_ATTRIBUTE_KEYS =
      List.of("http.request.path", "http.path", "http.target");
}
