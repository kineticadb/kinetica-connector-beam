package com.kinetica.beam.example;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.kinetica.beam.io.KineticaIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.gpudb.GPUdbException;
import com.gpudb.RecordObject;
import com.kinetica.beam.example.ExampleBeamPipeline.Scientist;

/**
 * Manipulates test data used by the {@link KineticaIO} tests.
 *
 * <p>This is independent of the tests so that for read tests it can be run separately after
 * data store creation rather than every time (which can be more fragile).
 */
public class KineticaTestDataSet {

  private static final Logger logger = LoggerFactory.getLogger(KineticaTestDataSet.class);

  private static KineticaTestDataSet singleton = null;

  private GPUdb gpudb = null;

  private KineticaTestDataSet() {
  }

  private KineticaTestDataSet(ExamplePipelineOptions pipelineOtions) {
    gpudb = getGPUdb(pipelineOtions);
  }

  public static KineticaTestDataSet factory(ExamplePipelineOptions pipelineOtions) {

    if (singleton == null)
      singleton = new KineticaTestDataSet(pipelineOtions);

    return singleton;
  }

  public static GPUdb getGPUdb(ExamplePipelineOptions options) {
    GPUdbBase.Options gpudbOptions = new GPUdbBase.Options();

    gpudbOptions.setUsername(options.getKineticaUsername());
    gpudbOptions.setPassword(options.getKineticaPassword());

    logger.info("*** Connecting to GPUDB at :" + options.getKineticaURL() + " as " + options.getKineticaUsername());

    GPUdb gpudb = null;
    try {
      gpudb = new GPUdb(options.getKineticaURL(), gpudbOptions);
    } catch (GPUdbException e) {
      logger.error("Can't connect to GPUdb", e);
      System.exit(99);
    }

    return gpudb;
  }

  public void testCreateTable(String tableName) throws GPUdbException {

    if (!gpudb.hasTable(tableName, GPUdb.options()).getTableExists()) {
      String scientistTypeId = RecordObject.createType(Scientist.class, gpudb);
      gpudb.createTable(tableName, scientistTypeId, GPUdb.options());
    }
  }

  public void testDropTable(String tableName) throws GPUdbException {

    if (gpudb.hasTable(tableName, GPUdb.options()).getTableExists()) {
      gpudb.clearTable(tableName, "", GPUdb.options());
    }
  }

  public static List<Scientist> getTestData() {

    String[] scientists = {
        "Lovelace",
        "Franklin",
        "Meitner",
        "Hopper",
        "Curie",
        "Faraday",
        "Newton",
        "Bohr",
        "Galilei",
        "Maxwell"
    };

    return IntStream.range(0, 1000)
        .mapToObj(i -> {
          int index = i % scientists.length;

          Scientist scientist = new Scientist();
          scientist.name = scientists[index];
          scientist.id = i;
          return scientist;
        }).collect(Collectors.toList());
  }
}
