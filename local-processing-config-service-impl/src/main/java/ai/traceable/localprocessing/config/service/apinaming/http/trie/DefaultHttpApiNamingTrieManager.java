package ai.traceable.localprocessing.config.service.apinaming.http.trie;

import ai.traceable.localprocessing.config.service.apinaming.http.utils.HttpApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.config.http.HttpApiNamingConfig;
import ai.traceable.localprocessing.config.service.v1.DiffTrie;
import ai.traceable.localprocessing.config.service.v1.FullTrie;
import ai.traceable.localprocessing.config.service.v1.Trie;
import ai.traceable.localprocessing.config.service.v1.TrieDiffLog;
import ai.traceable.platform.model.store.ServiceScope;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DefaultHttpApiNamingTrieManager implements HttpApiNamingTrieManager {

  private final HttpApiNamingConfig httpApiNamingConfig;
  private final FullTrieManager fullTrieManager;
  private final TrieDiffLogManager trieDiffLogManager;

  @Inject
  public DefaultHttpApiNamingTrieManager(
      HttpApiNamingConfig httpApiNamingConfig,
      FullTrieManager fullTrieManager,
      TrieDiffLogManager trieDiffLogManager) {
    this.httpApiNamingConfig = httpApiNamingConfig;
    this.fullTrieManager = fullTrieManager;
    this.trieDiffLogManager = trieDiffLogManager;
  }

  /**
   * Sends full trie based on the following conditions
   *
   * <ul>
   *   <li>Agent timestamp is 0 i.e agent makes request for the first time
   *   <li>Agent timestamp is 1hr(configurable) behind trie model timestamp i.e. agent timestamp is
   *       older than the retention period of diff log models
   *   <li>No diff logs and agent timestamp < trie model timestamp
   * </ul>
   */
  public Trie getTrie(
      RequestContext requestContext,
      HttpApiNamingConfigInfo httpApiNamingConfigInfo,
      String serviceId,
      String trieToken,
      long fullTrieReloadTimestamp)
      throws IOException, ExecutionException {
    long agentTimestampMillis = getAgentTimestampMillis(trieToken, serviceId, requestContext);
    String tenantId = requestContext.getTenantId().orElseThrow();
    ServiceScope serviceScope = new ServiceScope(tenantId, serviceId);
    List<TrieDiffLog> trieDiffLogs = Collections.emptyList();
    long trieModelTimestampMillis = fullTrieManager.getModelTimestamp(serviceScope);
    boolean reloadFullTrie = agentTimestampMillis < fullTrieReloadTimestamp;

    if (!reloadFullTrie
        && agentTimestampMillis != 0
        && trieModelTimestampMillis - agentTimestampMillis
            <= httpApiNamingConfig.getDiffLogsRetentionPeriod()) {
      trieDiffLogs =
          trieDiffLogManager.getAllTrieDiffLogs(requestContext, serviceScope, agentTimestampMillis);
    }

    if (!trieDiffLogs.isEmpty()) {
      return Trie.newBuilder()
          .setDiffTrie(getDiffTrie(trieDiffLogs))
          .setToken(
              String.valueOf(
                  trieDiffLogManager.getLatestDiffLogTimestamp(
                      requestContext, serviceScope, agentTimestampMillis)))
          .build();
    } else {
      if (agentTimestampMillis < trieModelTimestampMillis) {
        Optional<FullTrie> fullTrieMaybe =
            fullTrieManager.getFullTrie(requestContext, serviceScope, httpApiNamingConfigInfo);
        if (fullTrieMaybe.isEmpty()) {
          log.error(
              "Could not fetch full trie for request context:{}, service scope:{}",
              requestContext,
              serviceId);
          return Trie.newBuilder().setToken(trieToken).build();
        }
        return Trie.newBuilder()
            .setFullTrie(fullTrieMaybe.get())
            .setToken(String.valueOf(trieModelTimestampMillis))
            .build();
      } else {
        return Trie.newBuilder().setToken(trieToken).build();
      }
    }
  }

  private DiffTrie getDiffTrie(List<TrieDiffLog> allTrieDiffLogs) {
    return DiffTrie.newBuilder().addAllTrieDiffLogs(allTrieDiffLogs).build();
  }

  private long getAgentTimestampMillis(
      String trieToken, String serviceId, RequestContext requestContext) {
    try {
      return Long.parseLong(trieToken);
    } catch (NumberFormatException e) {
      log.error(
          "Could not parse trieToken:{}, for serviceId:{}, and requestContext:{}",
          trieToken,
          serviceId,
          requestContext);
      return 0;
    }
  }
}
