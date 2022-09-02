package com.kinetica.beam.test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.gpudb.GPUdbException;
import com.gpudb.RecordObject;
import com.kinetica.beam.test.IT_KineticaIOTest.Scientist;

/**
 * Manipulates test data used by the {@link KineticaIO} tests.
 *
 * <p>This is independent from the tests so that for read tests it can be run separately after
 * data store creation rather than every time (which can be more fragile).
 */
public class KineticaTestDataSet {

  private static final Logger LOG = LoggerFactory.getLogger(KineticaTestDataSet.class);

  /*
  static {	  
	    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
	    IOTestPipelineOptions pipelineOptions =
	        PipelineOptionsFactory.create().as(IOTestPipelineOptions.class);

	    LOG.debug("*** "+pipelineOptions.getKineticaURL());
		gpudb = getGPUdb(pipelineOptions);
  }
*/

  public static GPUdb getGPUdb(IOTestPipelineOptions options) {
    GPUdbBase.Options gpudbOptions = new GPUdbBase.Options();

    gpudbOptions.setUsername(options.getKineticaUsername());
    gpudbOptions.setPassword(options.getKineticaPassword());

    GPUdb gpudb = null;
    try {
      gpudb = new GPUdb(options.getKineticaURL(), gpudbOptions);
    } catch (GPUdbException e) {
      LOG.error("Can't connect to GPUdb", e);
      System.exit(99);
    }

    return gpudb;
  }

  public static void testCreateTable(GPUdb gpudb, String tableName) throws GPUdbException {

    if (!gpudb.hasTable(tableName, GPUdb.options()).getTableExists()) {
      String scientistTypeId = RecordObject.createType(Scientist.class, gpudb);
      gpudb.createTable(tableName, scientistTypeId, GPUdb.options());
    }
  }

  public static void testDropTable(GPUdb gpudb, String tableName) throws GPUdbException {

    if (gpudb.hasTable(tableName, GPUdb.options()).getTableExists()) {
      gpudb.clearTable(tableName, "", GPUdb.options());
    }
  }

  public static void insertTestData(GPUdb gpudb, String tableName) throws GPUdbException {

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

    List<Scientist> data = IntStream.range(0, 1000)
        .mapToObj(i -> {
          int index = i % scientists.length;

          Scientist scientist = new Scientist();
          scientist.name = scientists[index];
          scientist.id = i;
          return scientist;
        }).collect(Collectors.toList());

    gpudb.insertRecords(tableName, data, GPUdb.options());

  }

}
