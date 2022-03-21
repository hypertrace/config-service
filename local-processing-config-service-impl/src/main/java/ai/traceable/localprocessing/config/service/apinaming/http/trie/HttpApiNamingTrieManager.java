package ai.traceable.localprocessing.config.service.apinaming.http.trie;

import ai.traceable.localprocessing.config.service.apinaming.http.utils.HttpApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.v1.Trie;
import com.github.rholder.retry.RetryException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface HttpApiNamingTrieManager {
  Trie getTrie(
      RequestContext requestContext,
      HttpApiNamingConfigInfo httpApiNamingConfigInfo,
      String serviceId,
      String trieToken,
      long fullTrieReloadTimestamp)
      throws IOException, ExecutionException, RetryException;
}
