package com.kinetica.beam.example;


import com.kinetica.beam.io.KineticaIO;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.GPUdbException;

import com.google.common.base.Objects;
import com.gpudb.RecordObject;

public class ExampleBeamPipeline implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LoggerFactory.getLogger(ExampleBeamPipeline.class);

  public static void main(String[] args) {

    logger.info("Example starting...");

    ExamplePipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(ExamplePipelineOptions.class);

    logger.info(options.toString());

    testWrite(options);

    testRead(options);

    logger.info("Example complete!");

  }

  public static void testRead(ExamplePipelineOptions options) {

    // Can't reuse a pipeline - each test has its own
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Scientist> output = pipeline.apply(KineticaIO.<Scientist>read()
        .withHeadNodeURL(options.getKineticaURL())
        .withUsername(options.getKineticaUsername())
        .withPassword(options.getKineticaPassword())
        .withTable(options.getKineticaTable())
        .withEntity(Scientist.class)
        .withCoder(SerializableCoder.of(Scientist.class)));


    PCollection<Scientist> names = output.apply(
        ParDo.of(new ExampleBeamPipeline.LogScientists()));

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


    pipeline.run().waitUntilFinish();
  }

  public static void testWrite(ExamplePipelineOptions options) {

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

    pipeline
        .apply(Create.of(KineticaTestDataSet.getTestData()))
        .apply(KineticaIO.<Scientist>write()
            .withHeadNodeURL(options.getKineticaURL())
            .withTable(options.getKineticaTable())
            .withUsername(options.getKineticaUsername())
            .withPassword(options.getKineticaPassword())
            .withEntity(Scientist.class));

    pipeline.run().waitUntilFinish();
  }

  static class LogScientists extends DoFn<Scientist, Scientist> {

    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      Scientist scientist = c.element();
      logger.info("Name: " + scientist.name + ", id: " + scientist.id);
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

    @RecordObject.Column(order = 0, properties = {"char32"})
    public String name;

    @RecordObject.Column(order = 1, properties = {})
    public Integer id;

    @Override
    public String toString() {
      return id + ":" + name;
    }

    public Scientist() {
    }

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

