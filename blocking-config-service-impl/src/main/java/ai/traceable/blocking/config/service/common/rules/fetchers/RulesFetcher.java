package ai.traceable.blocking.config.service.common.rules.fetchers;

public interface RulesFetcher {

  enum RulesFetcherType {
    MALICIOUS_SOURCES,
    REGION,
    CUSTOM_SIGNATURE,
    DLP,
    EXCLUSION,
    IP_RESOLUTION_STRATEGY
  }
}
