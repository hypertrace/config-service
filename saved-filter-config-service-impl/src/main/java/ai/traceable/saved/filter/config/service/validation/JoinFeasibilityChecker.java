package ai.traceable.saved.filter.config.service.validation;

import com.google.inject.ImplementedBy;

@ImplementedBy(JoinFeasibilityCheckerImpl.class)
public interface JoinFeasibilityChecker {
  void validate(final String source, final String target);
}
