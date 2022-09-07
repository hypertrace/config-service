package ai.traceable.localprocessing.config.service.apinaming.http.trie;

import ai.traceable.localprocessing.config.service.apinaming.http.utils.HttpApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.config.http.HttpApiNamingConfig;
import ai.traceable.localprocessing.config.service.v1.ApiNamingPatterns;
import ai.traceable.localprocessing.config.service.v1.DiffLog;
import ai.traceable.localprocessing.config.service.v1.DiffPattern;
import ai.traceable.localprocessing.config.service.v1.FullPattern;
import ai.traceable.platform.model.store.ServiceScope;
import com.google.inject.Inject;
import java.io.IOException;
import java.time.Instant;
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
   *
   *
   * <ul>
   *   <li>Agent token is 0 i.e agent makes request for the first time
   *   <li>Agent timestamp is 1hr(configurable) behind trie model timestamp i.e. agent timestamp is
   *       older than the retention period of diff log models
   *   <li>No diff logs and agent timestamp < trie model timestamp
   *   <li>Sends full trie based on the following conditions Trie token - is timestamp of agent and
   *       version of trie separated by a comma
   *   <li>If version of platform trie is not equal to agent version full trie is sent
   * </ul>
   */
  public ApiNamingPatterns getApiNamingPatterns(
      RequestContext requestContext,
      HttpApiNamingConfigInfo httpApiNamingConfigInfo,
      String serviceId,
      String trieToken,
      String platformTrieVersion)
      throws IOException, ExecutionException {
    String tenantId = requestContext.getTenantId().orElseThrow();
    ServiceScope serviceScope = new ServiceScope(tenantId, serviceId);
    List<DiffLog> diffLogs = Collections.emptyList();
    long trieModelTimestampMillis = fullTrieManager.getModelTimestamp(serviceScope);
    long agentTimestampMillis = getAgentTimestampMillis(trieToken, serviceId, requestContext);
    boolean reloadFullTrie =
        !platformTrieVersion.equals(getAgentVersion(trieToken, serviceId, requestContext));

    if (!reloadFullTrie
        && agentTimestampMillis != 0
        && trieModelTimestampMillis - agentTimestampMillis
            <= httpApiNamingConfig.getDiffLogsRetentionPeriod()) {
      if (log.isDebugEnabled()) {
        log.debug(
            "Loading diff logs. request context:{}, serviceId:{}, AgentTimestamp:{}, trieModelTimestamp:{},diffLogRetentionPeriod:{}, reloadFullTrie:{}",
            requestContext,
            serviceId,
            Instant.ofEpochMilli(agentTimestampMillis),
            Instant.ofEpochMilli(trieModelTimestampMillis),
            httpApiNamingConfig.getDiffLogsRetentionPeriod(),
            reloadFullTrie);
      }
      diffLogs =
          trieDiffLogManager.getAllTrieDiffLogs(
              requestContext,
              serviceScope,
              agentTimestampMillis,
              httpApiNamingConfigInfo.getWildcardConfigMap());
    }

    if (!diffLogs.isEmpty()) {
      long latestDiffLogTimestamp =
          trieDiffLogManager.getLatestDiffLogTimestamp(
              requestContext, serviceScope, agentTimestampMillis);
      if (log.isDebugEnabled()) {
        log.debug(
            "Found diff logs. Request context:{}, serviceId:{}, latestDiffLogTimestamp:{} ",
            requestContext,
            serviceId,
            Instant.ofEpochMilli(latestDiffLogTimestamp));
      }
      return ApiNamingPatterns.newBuilder()
          .setDiffPattern(getDiffPatterns(diffLogs))
          .setToken("t=" + latestDiffLogTimestamp + ";v=" + platformTrieVersion)
          .build();
    } else {
      log.debug("Empty diff logs. Request context:{}, serviceId:{}", requestContext, serviceId);
      if (agentTimestampMillis < trieModelTimestampMillis) {
        if (log.isDebugEnabled()) {
          log.debug(
              "Reading full trie as agentTimestamp:{} < trieModelTimestamp:{} for request context:{}, serviceId:{}",
              Instant.ofEpochMilli(agentTimestampMillis),
              Instant.ofEpochMilli(trieModelTimestampMillis),
              requestContext,
              serviceId);
        }
        Optional<FullPattern> fullPatternMaybe =
            fullTrieManager.getFullPattern(requestContext, serviceScope, httpApiNamingConfigInfo);
        if (fullPatternMaybe.isEmpty()) {
          log.error(
              "Could not fetch full trie for request context:{}, service scope:{}",
              requestContext,
              serviceId);
          return ApiNamingPatterns.newBuilder().setToken(trieToken).build();
        }
        return ApiNamingPatterns.newBuilder()
            .setFullPattern(fullPatternMaybe.get())
            .setToken("t=" + trieModelTimestampMillis + ";v=" + platformTrieVersion)
            .build();
      } else {
        if (log.isDebugEnabled()) {
          log.debug(
              "Not setting full trie/diff logs as agentTimestamp:{} >= trieModelTimestamp:{} for request context:{}, serviceId:{}",
              Instant.ofEpochMilli(agentTimestampMillis),
              Instant.ofEpochMilli(trieModelTimestampMillis),
              requestContext,
              serviceId);
        }
        return ApiNamingPatterns.newBuilder().setToken(trieToken).build();
      }
    }
  }

  private DiffPattern getDiffPatterns(List<DiffLog> allDiffLogs) {
    return DiffPattern.newBuilder().addAllDiffLogs(allDiffLogs).build();
  }

  private long getAgentTimestampMillis(
      String trieToken, String serviceId, RequestContext requestContext) {
    try {
      if (trieToken.equals("")) {
        return 0;
      }
      String[] tokens = trieToken.split(";");
      return Long.parseLong(tokens[0].substring(2));
    } catch (Exception e) {
      log.error(
          "Could not parse timestamp from trieToken:{}, for serviceId:{}, and requestContext:{}",
          trieToken,
          serviceId,
          requestContext);
      return 0;
    }
  }

  private String getAgentVersion(
      String trieToken, String serviceId, RequestContext requestContext) {
    try {
      if (trieToken.equals("")) {
        return "0.0.0";
      }
      String[] tokens = trieToken.split(";");
      return tokens[1].substring(2);
    } catch (Exception e) {
      log.error(
          "Could not parse version from trieToken:{}, for serviceId:{}, and requestContext:{}",
          trieToken,
          serviceId,
          requestContext);
      return "0.0.0";
    }
  }
}
