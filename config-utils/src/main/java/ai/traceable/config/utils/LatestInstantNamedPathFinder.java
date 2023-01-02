package ai.traceable.config.utils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Fetches file path which has the latest instant in the name inside a directory with option to pass
 * file filter. Comparison is done at seconds, lower resolution will be ignored
 */
public class LatestInstantNamedPathFinder {
  public Optional<Path> get(URI directoryPath, Predicate<Path> filesFilter) {
    try (Stream<Path> dirPathStream = Files.list(Path.of(directoryPath))) {
      return dirPathStream
          .filter(filesFilter)
          .map(Path::toFile)
          .max(Comparator.comparing(file -> Instant.parse(file.getName()).getEpochSecond()))
          .map(File::toPath);
    } catch (IOException e) {
      return Optional.empty();
    }
  }
}
