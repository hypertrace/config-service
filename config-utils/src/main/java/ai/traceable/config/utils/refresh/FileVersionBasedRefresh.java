package ai.traceable.config.utils.refresh;

import ai.traceable.config.utils.LatestInstantNamedPathFinder;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.Resources;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

@Slf4j
public abstract class FileVersionBasedRefresh<T> {
  private final LatestInstantNamedPathFinder latestInstantNamedPathFinder;
  protected Supplier<T> supplier = null;

  protected FileVersionBasedRefresh(LatestInstantNamedPathFinder latestInstantNamedPathFinder) {
    this.latestInstantNamedPathFinder = latestInstantNamedPathFinder;
  }

  public Supplier<T> getLatestDataSupplier() {
    if (supplier == null) {
      throw new RuntimeException("VersionBasedRefreshConfig config not found");
    }
    return supplier;
  }

  protected void initializeSupplier(FileRefreshConfig config) {
    switch (config.getMode()) {
      case RESOURCE_FILE:
        this.supplier =
            () ->
                fetchRecordsAndApplyFunction(
                    config, latestInstantNamedPathFinder, this::buildFromRecords);
        break;
      case VERSIONS_DIR:
        // Using cache to ensure async refresh of data
        LoadingCache<FileRefreshConfig, T> cache =
            CacheBuilder.newBuilder()
                .refreshAfterWrite(config.getVersionRefreshDuration())
                .build(
                    CacheLoader.asyncReloading(
                        CacheLoader.from(
                            () ->
                                fetchRecordsAndApplyFunction(
                                    config, latestInstantNamedPathFinder, this::buildFromRecords)),
                        Executors.newSingleThreadExecutor()));
        // Seed the cache
        cache.put(
            config,
            fetchRecordsAndApplyFunction(
                config, latestInstantNamedPathFinder, this::buildFromRecords));
        this.supplier = () -> cache.getUnchecked(config);
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported VersionBasedRefreshConfig config mode for CsvVersionBasedRefresh");
    }
  }

  protected abstract T buildFromRecords(Iterable<CSVRecord> records);

  @SneakyThrows
  public static <K> K fetchRecordsAndApplyFunction(
      FileRefreshConfig config,
      LatestInstantNamedPathFinder latestInstantNamedPathFinder,
      Function<Iterable<CSVRecord>, K> applier) {
    switch (config.getMode()) {
      case RESOURCE_FILE:
        try (Reader reader =
            Resources.asCharSource(
                    Resources.getResource(config.getResourceFile()), Charset.defaultCharset())
                .openBufferedStream()) {
          return applier.apply(
              CSVFormat.DEFAULT.withHeader().withFirstRecordAsHeader().parse(reader));
        }
      case VERSIONS_DIR:
        Path latestDirPath =
            latestInstantNamedPathFinder
                .get(Path.of(config.getVersionsDir()).toUri(), Files::isDirectory)
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            "Could not fetch latest file in dir: " + config.getVersionsDir()));
        Path latestFilePath = latestDirPath.resolve(config.getVersionsFileName());
        log.info("Loading latest data from file {}", latestFilePath.toFile().getAbsolutePath());
        try (Reader reader = Files.newBufferedReader(latestFilePath)) {
          return applier.apply(
              CSVFormat.DEFAULT.withHeader().withFirstRecordAsHeader().parse(reader));
        }
      default:
        throw new UnsupportedOperationException(
            "Unknown FileRefreshConfig mode " + config.getMode());
    }
  }
}
