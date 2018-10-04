package com.kinetica.beam.io;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.gpudb.GPUdb;

import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

@Experimental(Experimental.Kind.SOURCE_SINK)
public class KineticaIO {

  private KineticaIO() {}

  /**
   * Provide a {@link Read} {@link PTransform} to read data from a Kinetica database.
   */
  public static <T extends com.gpudb.RecordObject> Read<T> read() {
    return new AutoValue_KineticaIO_Read.Builder<T>()
            .setKineticaService(new KineticaServiceImpl<>())
            .build();
  }

  /**
   * Provide a {@link Write} {@link PTransform} to write data to a Kinetica database.
   */
  public static <T extends com.gpudb.RecordObject> Write<T> write() {
    return new AutoValue_KineticaIO_Write.Builder<T>()
        .setKineticaService(new KineticaServiceImpl<>())
        .build();
  }

  /**
   * A {@link PTransform} to read data from Kinetica. See {@link KineticaIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

	private static final long serialVersionUID = 1L;
	
	@Nullable abstract String headNodeURL();
    @Nullable abstract String table();
    @Nullable abstract Class<T> entity();
    @Nullable abstract Coder<T> coder();
    @Nullable abstract String username();
    @Nullable abstract String password();
    @Nullable abstract Integer minNumberOfSplits();
    abstract KineticaService<T> kineticaService();
    abstract Builder<T> builder();

    /**
     * Specify the network address of the Kinetica headnode.
     */
    public Read<T> withHeadNodeURL(String headNodeURL) {
      checkArgument(headNodeURL != null, "headNodeURL can not be null");
      checkArgument(!headNodeURL.isEmpty(), "headNodeURL can not be empty");
      return builder().setHeadNodeURL(headNodeURL).build();
    }

    /**
     * Specify the Kinetica table where to read data.
     */
    public Read<T> withTable(String table) {
      checkArgument(table != null, "table can not be null");
      return builder().setTable(table).build();
    }

    /**
     * Specify the entity class (annotated POJO). The {@link KineticaIO} will read the data and
     * convert the data as entity instances. The {@link PCollection} resulting from the read will
     * contains entity elements.
     */
    public Read<T> withEntity(Class<T> entity) {
      checkArgument(entity != null, "entity can not be null");
      return builder().setEntity(entity).build();
    }

    /**
     * Specify the {@link Coder} used to serialize the entity in the {@link PCollection}.
     */
    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return builder().setCoder(coder).build();
    }

    /**
     * Specify the username for authentication.
     */
    public Read<T> withUsername(String username) {
      checkArgument(username != null, "username can not be null");
      return builder().setUsername(username).build();
    }

    /**
     * Specify the password for authentication.
     */
    public Read<T> withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      return builder().setPassword(password).build();
    }

    /**
     * Specify an instance of {@link KineticaService} used to connect and read from Kinetica
     * database.
     */
    public Read<T> withKineticaService(KineticaService<T> kineticaService) {
      checkArgument(kineticaService != null, "kineticaService can not be null");
      return builder().setKineticaService(kineticaService).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkArgument(
          (headNodeURL() != null && username() != null && password() != null) || kineticaService() != null,
          "Either withHeadNodeURL() and withUsername() and withPassword(), or withKineticaService() is required");
      checkArgument(table() != null, "withTable() is required");
      checkArgument(entity() != null, "withEntity() is required");
      checkArgument(coder() != null, "withCoder() is required");

      return input.apply(org.apache.beam.sdk.io.Read.from(new KineticaSource<>(this, null, 0, GPUdb.END_OF_SET)));
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setHeadNodeURL(String headNodeURL);
      abstract Builder<T> setTable(String table);
      abstract Builder<T> setEntity(Class<T> entity);
      abstract Builder<T> setCoder(Coder<T> coder);
      abstract Builder<T> setUsername(String username);
      abstract Builder<T> setPassword(String password);
      abstract Builder<T> setMinNumberOfSplits(Integer minNumberOfSplits);
      abstract Builder<T> setKineticaService(KineticaService<T> kineticaService);
      abstract Read<T> build();
    }
  }
  
  /**
   * A {@link PTransform} to write into Apache Kinetica. See {@link KineticaIO} for details on
   * usage and configuration.
   */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {
	  
	private static final long serialVersionUID = 1L;
	  
    @Nullable abstract String headNodeURL();
    @Nullable abstract String table();
    @Nullable abstract Class<T> entity();
    @Nullable abstract String username();
    @Nullable abstract String password();
    @Nullable abstract KineticaService<T> kineticaService();
    abstract Builder<T> builder();

    /**
     * Specify the Kinetica headnode where to write data.
     */
    public Write<T> withHeadNodeURL(String headNodeURL) {
      checkArgument(headNodeURL != null, "KineticaIO.write().withHeadNodeURL(headNodeURL) called with null hosts");
      checkArgument(!headNodeURL.isEmpty(), "KineticaIO.write().withHeadNodeURL(headNodeURL) called with empty URL");
      return builder().setHeadNodeURL(headNodeURL).build();
    }

    /**
     * Specify the Kinetica table where to write data.
     */
    public Write<T> withTable(String table) {
      checkArgument(table != null, "table can not be null");
      return builder().setTable(table).build();
    }

    /**
     * Specify the entity class in the input {@link PCollection}. The {@link KineticaIO} will
     * map this entity to the Kinetica table thanks to the annotations.
     */
    public Write<T> withEntity(Class<T> entity) {
      checkArgument(entity != null, "KineticaIO.write().withEntity(entity) called with null "
          + "entity");
      return builder().setEntity(entity).build();
    }

    /**
     * Specify the username used for authentication.
     */
    public Write<T> withUsername(String username) {
      checkArgument(username != null, "KineticaIO.write().withUsername(username) called with "
          + "null username");
      return builder().setUsername(username).build();
    }

    /**
     * Specify the password used for authentication.
     */
    public Write<T> withPassword(String password) {
      checkArgument(password != null, "KineticaIO.write().withPassword(password) called with "
          + "null password");
      return builder().setPassword(password).build();
    }

    /**
     * Specify the {@link KineticaService} used to connect and write into the Kinetica database.
     */
    public Write<T> withKineticaService(KineticaService<T> KineticaService) {
      checkArgument(KineticaService != null, "KineticaIO.write().withKineticaService"
          + "(service) called with null service");
      return builder().setKineticaService(KineticaService).build();
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {
      checkState(headNodeURL() != null && username() != null && password() != null || kineticaService() != null,
          "KineticaIO.write() requires a headnode URL, username and password s to be set via withheadNodeURL(), withUsername and withPassword() or a "
              + "Kinetica service to be set via withKineticaService(service)");
      checkState(table() != null, "KineticaIO.write() requires a table to be set via "
              + "withTable(tablename)");
      checkState(entity() != null, "KineticaIO.write() requires an entity to be set via "
          + "withEntity(entity)");
    }

    @Override
    public PDone expand(PCollection<T> input) {
      input.apply(ParDo.of(new WriteFn<>(this)));
      return PDone.in(input.getPipeline());
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setHeadNodeURL(String headNodeURL);
      abstract Builder<T> setTable(String table);
      abstract Builder<T> setEntity(Class<T> entity);
      abstract Builder<T> setUsername(String username);
      abstract Builder<T> setPassword(String password);
      abstract Builder<T> setKineticaService(KineticaService<T> kineticaService);
      abstract Write<T> build();
    }
  }

  private static class WriteFn<T> extends DoFn<T, Void> {
	  
	private static final long serialVersionUID = 1L;
	  
    private final Write<T> spec;
    private KineticaService.Writer<T> writer;

    WriteFn(Write<T> spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      writer = spec.kineticaService().createWriter(spec);
    }

    @ProcessElement
    public void processElement(ProcessContext c)
        throws ExecutionException, InterruptedException {
      writer.write(c.element());
    }

    @Teardown
    public void teardown() throws Exception {
      writer.close();
      writer = null;
    }
  }
  
  @VisibleForTesting
  static class KineticaSource<T> extends BoundedSource<T> {
	  
	private static final long serialVersionUID = 1L;
	
    final Read<T> spec;

    final String tableName;
    final long offset;
    final long limit;
    
    KineticaSource(Read<T> spec, String tableName, long offset, long limit) {
        this.spec = spec;
        this.tableName = tableName;
        this.offset = offset;
        this.limit = limit;
      }

    @Override
    public Coder<T> getOutputCoder() {
      return spec.coder();
    }

    @Override
    public BoundedReader<T> createReader(PipelineOptions pipelineOptions) {
      return spec.kineticaService().createReader(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) {
      return spec.kineticaService().getEstimatedSizeBytes(spec);
    }

    @Override
    public List<BoundedSource<T>> split(
        long desiredBundleSizeBytes, PipelineOptions pipelineOptions) {
      return spec.kineticaService().split(spec, desiredBundleSizeBytes);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      if (spec.headNodeURL() != null) {
        builder.add(DisplayData.item("headNodeURL", spec.headNodeURL().toString()));
      }
      builder.addIfNotNull(DisplayData.item("table", spec.table()));
      builder.addIfNotNull(DisplayData.item("username", spec.username()));
    }
  }
}
