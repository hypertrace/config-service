package ai.traceable.external.agent.attribute.config.service;

public class ExternalAgentAttributeConfigServiceConstants {
  public static final String PROJECTOR_PREDICATE_SUPPORT_MIN_TPA_VERSION =
      "1.30.0-rc.0"; // This version has support for ProjectorPredicate and the bug fix that
  // AttributeAddition Action will be no-op if ValueProjection fails.

  public static final String KEY_PREDICATE_SUPPORT_MIN_TPA_VERSION =
      "1.43.0-rc.0"; // key predicate support for cooki and url projector
  // https://traceableai.atlassian.net/browse/ENG-40200
}
