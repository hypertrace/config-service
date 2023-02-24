package ai.traceable.blocking.config.service.common.util;

import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SemanticVersioningComparator implements Comparator<String> {

  /**
   * Assumptions of version format, where N is a number Version are either N.N.N or N.N.N-word.N
   *
   * <p>Now for comparison 1. N+1.N.N > N.N+1.N > N.N.N+1 > N.N.N > N.N.N-word.N+1 > N.N.N-word.N 2.
   * N.N.N is greater than any version with suffix with same primary version 3. Words are compared
   * using lexicographical ordering
   *
   * <p>Example -> For TPA versions we have a notion where the version ordered as follows - 1.8.2 >
   * 1.8.2-rc.2 > 1.7.1 > 1.7.1-rc.1 > 1.7.1-dev.27 > 1.7.1-dev.2
   */
  private static final String SEMVER_VERSION_REGEX =
      "^([\\d]+)\\.([\\d]+)\\.([\\d]+)(-(.*)\\.([\\d]+))?$";

  private static final Pattern SEMVER_VERSION_PATTERN = Pattern.compile(SEMVER_VERSION_REGEX);

  @Override
  public int compare(String a, String b) {
    // Assumes strings are of form
    Matcher matcherA = SEMVER_VERSION_PATTERN.matcher(a);
    Matcher matcherB = SEMVER_VERSION_PATTERN.matcher(b);

    if (!matcherA.matches()) {
      log.debug("Invalid format for the version {}", a);
    }

    if (!matcherB.matches()) {
      log.debug("Invalid format for the version {}", b);
    }

    int segmentComp = intCompare(matcherA.group(1), matcherB.group(1));
    if (segmentComp != 0) return segmentComp;

    segmentComp = intCompare(matcherA.group(2), matcherB.group(2));
    if (segmentComp != 0) return segmentComp;

    segmentComp = intCompare(matcherA.group(3), matcherB.group(3));
    if (segmentComp != 0) return segmentComp;

    if (matcherA.group(4) == null) {
      if (matcherB.group(4) == null) {
        return 0;
      }
      return 1;
    }
    if (matcherB.group(4) == null) {
      return -1;
    }

    if (!matcherA.group(5).equals(matcherB.group(5))) {
      return matcherA.group(5).compareTo(matcherB.group(5));
    }
    return intCompare(matcherA.group(6), matcherB.group(6));
  }

  private static int intCompare(String a, String b) {
    return Integer.parseInt(a) - Integer.parseInt(b);
  }
}
