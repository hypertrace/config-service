package ai.traceable.localprocessing.config.service.apinaming.http.trie;

import ai.traceable.localprocessing.config.service.apinaming.http.utils.HttpApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.v1.ApiNamingPatterns;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface HttpApiNamingTrieManager {
  ApiNamingPatterns getApiNamingPatterns(
      RequestContext requestContext,
      HttpApiNamingConfigInfo httpApiNamingConfigInfo,
      String serviceId,
      String trieToken,
      String platformTrieVersion)
      throws IOException, ExecutionException;
}
