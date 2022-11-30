package ai.traceable.config.utils.refresh;

import ai.traceable.config.utils.LastModifiedPathFinder;
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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public abstract class FileVersionBasedRefresh<T> {
  private final LastModifiedPathFinder lastModifiedPathFinder;

  protected FileVersionBasedRefresh(LastModifiedPathFinder lastModifiedPathFinder) {
    this.lastModifiedPathFinder = lastModifiedPathFinder;
  }

  public Supplier<T> getLatestDataSupplier(FileRefreshConfig config) {
    switch (config.getMode()) {
      case RESOURCE_FILE:
        T dataFromResources = fetchDataFromResources(config.getResourceFile());
        return () -> dataFromResources;
      case VERSIONS_DIR:
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
        return () -> cache.getUnchecked(config);
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
    Path lastModifiedPath =
        lastModifiedPathFinder
            .get(Path.of(config.getVersionsDir()).toUri(), Files::isDirectory)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Could not fetch latest file in dir: " + config.getVersionsDir()));

    Reader reader = Files.newBufferedReader(lastModifiedPath.resolve(config.getVersionsFileName()));
    return buildFromRecords(CSVFormat.DEFAULT.withHeader().withFirstRecordAsHeader().parse(reader));
  }
}
