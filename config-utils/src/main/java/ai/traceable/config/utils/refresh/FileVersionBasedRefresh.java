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
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

@Slf4j
public abstract class FileVersionBasedRefresh<T> {
  private final LatestInstantNamedPathFinder latestInstantNamedPathFinder;
  private Supplier<T> supplier = null;

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
        T dataFromResources = fetchDataFromResources(config.getResourceFile());
        this.supplier = () -> dataFromResources;
        break;
      case VERSIONS_DIR:
        // Using cache to ensure async refresh of data
        LoadingCache<FileRefreshConfig, T> cache =
            CacheBuilder.newBuilder()
                .refreshAfterWrite(config.getVersionRefreshDuration())
                .build(
                    CacheLoader.asyncReloading(
                        CacheLoader.from(this::fetchDataFromLatestFile),
                        Executors.newSingleThreadExecutor()));
        // Seed the cache
        T dataFromVersionedFile = fetchDataFromLatestFile(config);
        cache.put(config, dataFromVersionedFile);
        this.supplier = () -> cache.getUnchecked(config);
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported VersionBasedRefreshConfig config mode for CsvVersionBasedRefresh");
    }
  }

  protected abstract T buildFromRecords(Iterable<CSVRecord> records);

  @SneakyThrows
  private T fetchDataFromResources(String resourceDataPath) {
    Reader csvReader =
        Resources.asCharSource(Resources.getResource(resourceDataPath), Charset.defaultCharset())
            .openBufferedStream();
    Iterable<CSVRecord> records =
        CSVFormat.DEFAULT.withHeader().withFirstRecordAsHeader().parse(csvReader);
    return buildFromRecords(records);
  }

  @SneakyThrows
  private T fetchDataFromLatestFile(FileRefreshConfig config) {
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
      return buildFromRecords(
          CSVFormat.DEFAULT.withHeader().withFirstRecordAsHeader().parse(reader));
    }
  }
}
