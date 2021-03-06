package com.kinetica.beam.example;

import static org.junit.Assert.assertEquals;

import com.kinetica.beam.io.KineticaIO;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;

import com.google.common.base.Objects;
import com.gpudb.RecordObject;

public class Test1 implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LoggerFactory.getLogger(Test1.class);

  public static void main(String[] args) {
	  
	logger.info("Starting");
	
	IOTestPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(IOTestPipelineOptions.class);
				
	logger.info(options.toString());

	testRead(options);
	
	testWrite(options);
	
  }
  
  public static void testRead(IOTestPipelineOptions options) {

	// Can't reuse a pipeline - each test has its own
	Pipeline pipeline = Pipeline.create(options);
		
	String tableName = options.getKineticaTable();
	try {
		KineticaTestDataSet dataSet = KineticaTestDataSet.factory(options);
		dataSet.testDropTable(tableName);
		dataSet.testCreateTable(tableName);
		dataSet.insertTestData(tableName);
	} catch (GPUdbException e) {
		throw new AssertionError(e);
	}
    
    PCollection<Scientist> output = pipeline.apply(KineticaIO.<Scientist>read()
        .withHeadNodeURL(options.getKineticaURL())
        .withUsername(options.getKineticaUsername())
        .withPassword(options.getKineticaPassword())
        .withTable(options.getKineticaTable())
        .withEntity(Scientist.class)
        .withCoder(SerializableCoder.of(Scientist.class)));


    PCollection<Scientist> names = output.apply(
		ParDo.of(new Test1.LogScientists()));

    
    PAssert.thatSingleton(output.apply("Count scientist", Count.globally())).isEqualTo(1000L);

    PCollection<KV<String, Integer>> mapped =
        output.apply(
            MapElements.via(
                new SimpleFunction<Scientist, KV<String, Integer>>() {
                  @Override
                  public KV<String, Integer> apply(Scientist scientist) {
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

  public static void testWrite(IOTestPipelineOptions options) {

	// Can't reuse a pipeline - each test has its own
	Pipeline pipeline = Pipeline.create(options);
			
	String tableName = options.getKineticaTable();
	try {
		KineticaTestDataSet dataSet = KineticaTestDataSet.factory(options);
		dataSet.testDropTable(tableName);
		dataSet.testCreateTable(tableName);
	} catch (GPUdbException e) {
		throw new AssertionError(e);
	}
    
    options.setOnSuccessMatcher(
        new KineticaMatcher(
            KineticaTestDataSet.getGPUdb(options),
            options.getKineticaTable()));

    ArrayList<Scientist> data = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Scientist scientist = new Scientist();
      scientist.id = i;
      scientist.name="Name " + i;
      data.add(scientist);
    }

    pipeline
        .apply(Create.of(data))
        .apply(KineticaIO.<Scientist>write()
            .withHeadNodeURL(options.getKineticaURL())
            .withTable(options.getKineticaTable())
            .withUsername(options.getKineticaUsername())
            .withPassword(options.getKineticaPassword())
            .withEntity(Scientist.class));

    pipeline.run().waitUntilFinish();
  }

  static class KineticaMatcher extends TypeSafeMatcher<PipelineResult> implements SerializableMatcher<PipelineResult> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(KineticaMatcher.class);
    private final String tableName;
    private final GPUdb gpudb;

    KineticaMatcher(GPUdb gpudb, String tableName) {
      this.gpudb = gpudb;
      this.tableName = tableName;
    }

    @Override
    protected boolean matchesSafely(PipelineResult pipelineResult) {
      pipelineResult.waitUntilFinish();

      try {
		List<Scientist> rows = gpudb.<Scientist>getRecords(tableName, 0, GPUdb.END_OF_SET, GPUdb.options()).getData();

		if (rows.size() != 1000) {
			return false;
		}
		for (Scientist scientist : rows) {
			if (!scientist.name.matches("Name.*")) {
				return false;
			}
		}
	  } catch (GPUdbException e) {
		LOG.error("failed talking to GPUdb", e);
		return false;
	  }

      return true;
  }

    @Override
    public void describeTo(Description description) {
      description.appendText("Expected Kinetica record pattern is (Name.*)");
    }
  }

  static class LogScientists extends DoFn<Scientist, Scientist> {
	  
	  private static final long serialVersionUID = 1L;
	  
	  @ProcessElement
	  public void processElement(ProcessContext c) {
		  Scientist scientist = c.element();
 		  logger.info("Name: "+scientist.name+", id: "+scientist.id);
	  }
  }
  
/**
   * Simple Kinetica entity representing a scientist for read/write to Kinetica database
   * Classes must:
   * - Be static class if nested, or declared at top level
   * - Have a public default constructor
   * - Extend the type RecordObject
   * - All fields mapping to Kinetica columns must be tagged using @RecordObject.Column and be accessible (public, not final or static)
   * - Define equality and hashCode functions
   */
  public static class Scientist extends RecordObject implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	@RecordObject.Column(order = 0, properties = { "char32" })
    public String name;

	@RecordObject.Column(order = 1, properties = { })
    public Integer id;

    @Override
    public String toString() {
      return id + ":" + name;
    }
    
    public Scientist() {}
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Scientist scientist = (Scientist) o;
      return Objects.equal(id, scientist.id) && Objects.equal(name, scientist.name);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, id);
    }

  }
}

