package ai.traceable.config.utils;

public class ExternalAgentAttributeConfigServiceConstants {
  public static final String PROJECTOR_PREDICATE_SUPPORT_MIN_TPA_VERSION =
      "1.30.0-rc.0"; // This version has support for ProjectorPredicate and the bug fix that
  // AttributeAddition Action will be no-op if ValueProjection fails.

  public static final String JSON_PATH_EXTRACTION_FIX_MIN_TPA_VERSION =
      "1.39.0-rc.0"; // json path extraction failure should return empty
  // previously itwas returning the complete json
  // https://traceableai.atlassian.net/browse/ENG-37015
}
