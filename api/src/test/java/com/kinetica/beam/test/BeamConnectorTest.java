package com.kinetica.beam.test;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.kinetica.beam.io.KineticaIO;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class BeamConnectorTest implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(IT_KineticaIOTest.class);

  private static IOTestPipelineOptions options;

  @Rule
  public transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() {

    LOG.debug("Starting");

    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions()
        .as(IOTestPipelineOptions.class);

    options.setStableUniqueNames(PipelineOptions.CheckEnabled.WARNING);

    LOG.debug(options.toString());
  }

  @AfterClass
  public static void tearDown() {
  }

  @Test
  public void testRead() {

    LOG.debug("Starting read test");

    GPUdb gpudb = KineticaTestDataSet.getGPUdb(options);
    String tableName = options.getKineticaTable();
    try {
      KineticaTestDataSet.testDropTable(gpudb, tableName);
      KineticaTestDataSet.testCreateTable(gpudb, tableName);
      KineticaTestDataSet.insertTestData(gpudb, tableName);
    } catch (GPUdbException e) {
      throw new AssertionError(e);
    }

    PCollection<IT_KineticaIOTest.Scientist> output = pipeline.apply(KineticaIO.<IT_KineticaIOTest.Scientist>read()
        .withHeadNodeURL(options.getKineticaURL())
        .withUsername(options.getKineticaUsername())
        .withPassword(options.getKineticaPassword())
        .withTable(options.getKineticaTable())
        .withEntity(IT_KineticaIOTest.Scientist.class)
        .withCoder(SerializableCoder.of(IT_KineticaIOTest.Scientist.class)));

// uncomment to dump out kinetica table data to log
//    PCollection<Scientist> names = output.apply(
//		ParDo.of(new IT_KineticaIOTest.LogScientists()));


    PAssert.thatSingleton(output.apply("Count scientist", Count.globally())).isEqualTo(1000L);

    PCollection<KV<String, Integer>> mapped =
        output.apply(
            MapElements.via(
                new SimpleFunction<IT_KineticaIOTest.Scientist, KV<String, Integer>>() {
                  @Override
                  public KV<String, Integer> apply(IT_KineticaIOTest.Scientist scientist) {
                    return KV.of(scientist.name, scientist.id);
                  }
                }
            )
        );

    PAssert.that(mapped.apply("Count occurrences per scientist", Count.perKey()))
        .satisfies(
            input -> {
              for (KV<String, Long> element : input) {
                assertEquals(element.getKey(), 100, element.getValue().longValue());
              }
              return null;
            });

    pipeline.run().waitUntilFinish();
  }

}
