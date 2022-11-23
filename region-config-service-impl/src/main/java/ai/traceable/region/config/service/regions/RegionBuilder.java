package ai.traceable.region.config.service.regions;

import ai.traceable.config.utils.LastModifiedPathFinder;
import ai.traceable.region.config.service.RegionConfigServiceConfig;
import ai.traceable.region.config.service.RegionConfigServiceConfig.CountriesDataConfig;
import ai.traceable.region.config.service.utils.UuidGenerator;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

@Slf4j
class RegionBuilder {
  private static final String START_IP_INT_CSV_HEADER = "start_ip_int";
  private static final String END_IP_INT_CSV_HEADER = "end_ip_int";
  private static final String COUNTRY_CSV_HEADER = "country";

  private final UuidGenerator uuidGenerator;
  private final LastModifiedPathFinder lastModifiedPathFinder;

  @Inject
  RegionBuilder(UuidGenerator uuidGenerator, LastModifiedPathFinder lastModifiedPathFinder) {
    this.uuidGenerator = uuidGenerator;
    this.lastModifiedPathFinder = lastModifiedPathFinder;
  }

  public Supplier<Map<String, Region>> buildRegions(
      RegionConfigServiceConfig.CountriesDataConfig dataConfig) {
    switch (dataConfig.getMode()) {
      case RESOURCE_FILE:
        Map<String, Region> resourceRegionData =
            buildRegionsFromResources(dataConfig.getResourceFile());
        return () -> resourceRegionData;
      case VERSIONS_DIR:
        LoadingCache<RegionConfigServiceConfig.CountriesDataConfig, Map<String, Region>> cache =
            CacheBuilder.newBuilder()
                .refreshAfterWrite(dataConfig.getVersionRefreshDuration())
                .build(
                    CacheLoader.asyncReloading(
                        CacheLoader.from(this::buildRegionsFromLatestFile),
                        Executors.newSingleThreadExecutor()));
        // Seed the cache
        Map<String, Region> versionedRegionData = buildRegionsFromLatestFile(dataConfig);
        cache.put(dataConfig, versionedRegionData);
        return () -> cache.getUnchecked(dataConfig);
      default:
        throw new IllegalArgumentException("Unsupported data config mode for CountriesDataConfig");
    }
  }

  @SneakyThrows
  private Map<String, Region> buildRegionsFromResources(String countriesResourceDataPath) {
    Reader csvReader =
        Resources.asCharSource(
                Resources.getResource(countriesResourceDataPath), Charset.defaultCharset())
            .openBufferedStream();
    Iterable<CSVRecord> records =
        CSVFormat.DEFAULT.withHeader().withFirstRecordAsHeader().parse(csvReader);
    return buildRegionsFromRecords(records);
  }

  @SneakyThrows
  private Map<String, Region> buildRegionsFromLatestFile(CountriesDataConfig config) {
    Path lastModifiedPath =
        lastModifiedPathFinder
            .get(Path.of(config.getVersionsDir()).toUri(), Files::isDirectory)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Could not fetch latest file in dir: " + config.getVersionsDir()));

    Reader reader = Files.newBufferedReader(lastModifiedPath.resolve(config.getVersionsFileName()));
    return buildRegionsFromRecords(
        CSVFormat.DEFAULT.withHeader().withFirstRecordAsHeader().parse(reader));
  }

  private Map<String, Region> buildRegionsFromRecords(Iterable<CSVRecord> records) {
    Map<String, Region> regionIdToRegionMap = new HashMap<>();
    for (CSVRecord record : records) {
      try {
        long startIp = Long.parseLong(record.get(START_IP_INT_CSV_HEADER));
        long endIp = Long.parseLong(record.get(END_IP_INT_CSV_HEADER));
        String country = record.get(COUNTRY_CSV_HEADER);
        String regionId = uuidGenerator.generateId(country);

        if (!regionIdToRegionMap.containsKey(regionId)) {
          Region region = new Region(regionId, country, RegionType.COUNTRY, new ArrayList<>());
          regionIdToRegionMap.put(regionId, region);
        }

        Region region = regionIdToRegionMap.get(regionId);
        region.getIpV4Ranges().add(new IpV4Range(startIp, endIp));

      } catch (Exception e) {
        log.error("Unable to parse csv record {}", record, e);
      }
    }

    return ImmutableMap.<String, Region>builder().putAll(regionIdToRegionMap).build();
  }
}
