package ai.traceable.customsignature.config.service.rules;

import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import lombok.AllArgsConstructor;
import lombok.Getter;

/** Helper class to return both updated ClauseGroup and whether changes were made. */
@AllArgsConstructor
@Getter
class ClauseGroupUpdateResult {
  private final ClauseGroup clauseGroup;
  private final boolean updated;
}
