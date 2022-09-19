package ai.traceable.region.config.service.regions;

import ai.traceable.region.config.service.utils.UuidGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

@Slf4j
class RegionBuilder {
  private static final String START_IP_INT_CSV_HEADER = "start_ip_int";
  private static final String END_IP_INT_CSV_HEADER = "end_ip_int";
  private static final String COUNTRY_CSV_HEADER = "country";

  private final UuidGenerator uuidGenerator;

  @Inject
  RegionBuilder(UuidGenerator uuidGenerator) {
    this.uuidGenerator = uuidGenerator;
  }

  // reads the CSV data and converts it into a region map
  public Map<String, Region> buildRegions(String countriesDataPath) {
    try {
      Reader csvReader = new InputStreamReader(getClass().getResourceAsStream(countriesDataPath));

      Iterable<CSVRecord> records =
          CSVFormat.DEFAULT.withHeader().withFirstRecordAsHeader().parse(csvReader);
      return buildRegions(records);
    } catch (FileNotFoundException e) {
      log.error("Unable to find countries data file {}", countriesDataPath, e);
    } catch (IOException e) {
      log.error("Unable to parse data file {}", countriesDataPath, e);
    }

    return Collections.emptyMap();
  }

  public Map<String, Region> buildRegions(Iterable<CSVRecord> records) {
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
