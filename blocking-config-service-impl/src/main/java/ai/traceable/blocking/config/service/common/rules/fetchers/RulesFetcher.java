package ai.traceable.blocking.config.service.common.rules.fetchers;

public interface RulesFetcher<T> {

  enum RulesFetcherType {
    MALICIOUS_SOURCES,
    REGION,
    CUSTOM_SIGNATURE,
    DLP;
  }
}
