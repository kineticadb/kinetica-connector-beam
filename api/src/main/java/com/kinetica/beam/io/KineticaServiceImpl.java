/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kinetica.beam.io;

import com.gpudb.BulkInserter;
import com.gpudb.BulkInserter.InsertException;
import com.gpudb.WorkerList;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.gpudb.GPUdbException;
import com.gpudb.GenericRecord;
import com.gpudb.RecordObject;
import com.gpudb.protocol.GetRecordsRequest;
import com.gpudb.protocol.GetRecordsResponse;
import com.gpudb.protocol.InsertRecordsResponse;
import com.gpudb.RecordObject.Column;
//import com.kinetica.beam.annotations.Column;
import com.gpudb.Type;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.beam.sdk.io.BoundedSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the {@link KineticaService} that actually use a Kinetica instance.
 */
public class KineticaServiceImpl<T extends RecordObject> implements KineticaService<T> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(KineticaServiceImpl.class);

  private class KineticaReaderImpl extends BoundedSource.BoundedReader<T> {
	  
    private final KineticaIO.KineticaSource<T> source;

    private Iterator<GenericRecord> iterator;
    private T current;

	private class KineticaValue {
	  	Class<?> javaType;
	  	String kineticaColumnName;
	  	Object value;
    }
  
	KineticaReaderImpl(KineticaIO.KineticaSource<T> source) {
      this.source = source;
    }

    @Override
    public boolean start() {
      LOG.debug("Starting Kinetica reader");

      // create a query to pull the split data from Kinetica
      GetRecordsRequest request = new GetRecordsRequest();
      request.setTableName(source.tableName);
      request.setOffset(source.offset);
      request.setLimit(source.limit);
      request.setEncoding(GetRecordsRequest.Encoding.BINARY);
      request.setOptions(new HashMap<String,String>());

      try {
		GetRecordsResponse<GenericRecord> response = getGPUdb().<GenericRecord>getRecords(request);
		iterator = response.getData().iterator();
        return advance();
      } catch (GPUdbException e) {
		LOG.error("failed reading data from Kinetica", e);
		return false;
      }
    }

    @Override
    public boolean advance() {
      if (iterator.hasNext()) {
    	GenericRecord gr = iterator.next();

    	Class<T> entityClass = source.spec.entity();

		try {
			// first retrieve the record
			Field[] fields = entityClass.getDeclaredFields();
			
			// find the java fields on type T that correspond to kinetica fields
			List<KineticaValue> kineticaRecord = Arrays.stream(fields).map(field -> {
				Column annnotation = (Column)field.getAnnotation(Column.class);
				if (annnotation != null) {
	    			String kineticaColumnName = annnotation.name();
	    			if(kineticaColumnName==null || kineticaColumnName.length()==0)
	    				kineticaColumnName=field.getName();

	    			KineticaValue kineticaValue = new KineticaValue();
    				kineticaValue.javaType = field.getType();
    				kineticaValue.value = gr.get(kineticaColumnName);
    				kineticaValue.kineticaColumnName = kineticaColumnName;	  
    				return kineticaValue;		    			
				} // ignore fields without @Colunn annotations
				return null;
			}).filter(Objects::nonNull).collect(Collectors.toList());

			// now find a default constructor, create an instance and set the values
			Constructor<T> constructor = entityClass.getDeclaredConstructor();
			if (constructor != null) {
				current = constructor.newInstance();
				kineticaRecord.forEach(kineticaValue -> {
					try {
						entityClass.getDeclaredField(kineticaValue.kineticaColumnName).set(current, kineticaValue.value);
					} catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException
							| SecurityException e) {
						LOG.error("Failed setting value on field",e);
					}
				});
			} else throw new Exception ("No public default constructor on class "+entityClass.getCanonicalName());
	    	
	        return true;
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}		
      }
      current = null;
      return false;
    }

    @Override
    public void close() {
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public KineticaIO.KineticaSource<T> getCurrentSource() {
      return source;
    }

    private GPUdb getGPUdb() {
    	GPUdbBase.Options gpudbOptions = new GPUdbBase.Options();

    	gpudbOptions.setUsername(source.spec.username());
    	gpudbOptions.setPassword(source.spec.password());
    		
    	GPUdb gpudb = null;
    	try {
    		gpudb = new GPUdb(source.spec.headNodeURL(), gpudbOptions);
    	} catch (GPUdbException e) {
    		LOG.error("Can't connect to GPUdb", e);
    		System.exit(99);
    	}
    	
    	return gpudb;
      }


  }

  @Override
  public KineticaReaderImpl createReader(KineticaIO.KineticaSource<T> source) {
    return new KineticaReaderImpl(source);
  }

  @Override
  public long getEstimatedSizeBytes(KineticaIO.Read<T> spec) {
	  return 0L;
  }

  @Override
  public List<BoundedSource<T>> split(KineticaIO.Read<T> spec,
      long desiredBundleSizeBytes) {
	  
	// For now we read everything in one split and one API call.
	// In future, it will be faster on large datasets to use a distributed UDF to read
	// multiple streams in parallel and map splits to kinetica ranks/toms
    List<BoundedSource<T>> sourceList = new ArrayList<BoundedSource<T>>();
    
    long estimatedSizeBytes = getEstimatedSizeBytes(spec);
    long numSplits = getNumSplits(desiredBundleSizeBytes, estimatedSizeBytes, spec.minNumberOfSplits());

    // pull the whole dataset in a single query for now
    sourceList.add(new KineticaIO.KineticaSource<T>(spec, spec.table(), 0, GPUdbBase.END_OF_SET));
    return sourceList;
  }

  private static long getNumSplits(
	      long desiredBundleSizeBytes, long estimatedSizeBytes, @Nullable Integer minNumberOfSplits) {

    long numSplits = desiredBundleSizeBytes > 0 ? (estimatedSizeBytes / desiredBundleSizeBytes) : 1;
    if (numSplits <= 0) {
      LOG.warn("Number of splits is less than 0 ({}), fallback to 1", numSplits);
      numSplits = 1;
    }
    return minNumberOfSplits != null ? Math.max(numSplits, minNumberOfSplits) : numSplits;
  }

  /**
   * Writer storing an entity into Kinetica database.
   */
  protected class KineticaWriterImpl implements Writer<T> {

    private final KineticaIO.Write<T> spec;

    private BulkInserter<T> bulkInserter = null;
    private final static int batchSize = 100000; // buffer size before the data is flushed to Kinetica, in units of number of records 
    private final static long flushTimerFrequency = 3000; // frequency in ms that residual data in the buffer is flushed to Kinetica
    
    KineticaWriterImpl(KineticaIO.Write<T> spec) {
      this.spec = spec;
    }

    /**
     * Write the entity to the Kinetica instance.
     * Note when using bulkInserter not all records will be flushed until ????
     */
    @Override
    public void write(T entity) throws ExecutionException, InterruptedException {

		String kineticaTableName = spec.table();
	
		try {
//			InsertRecordsResponse response = getGPUdb().insertRecords(kineticaTableName, Collections.singletonList(entity), null);

			if (bulkInserter==null) {
				GPUdb gpudb = getGPUdb();
				WorkerList workers = new WorkerList(gpudb);
				
				Type T_Type = RecordObject.getType(entity.getClass());
				
				bulkInserter = new BulkInserter<T>(getGPUdb(), kineticaTableName, T_Type, batchSize, null, workers);

				// commit all buffered data to Kinetica periodically even if the buffer is not full
				TimerTask flushTask = new TimerTask() {
		            @Override
		            public void run() {
		            	try {
		            		if (bulkInserter != null)
		            			bulkInserter.flush();
						} catch (InsertException e) {
							LOG.error("Failed writing to Kinetica on Writer timeout", e);
						}
		            }
		        };

		        Timer flushTimer = new Timer("FlushTimer", true);
		        flushTimer.schedule(flushTask, flushTimerFrequency /* delay */, flushTimerFrequency /* period */);
			}

			bulkInserter.insert(entity);
						
		} catch (GPUdbException e) {
			LOG.error("Failed writing to Kinetica");
			throw new ExecutionException(e);
		}
		
    }

    @Override
    public void close() throws ExecutionException, InterruptedException {

		// commit all buffered data to Kinetica before exit
    	try {
			bulkInserter.flush();
		} catch (InsertException e) {
			LOG.error("Failed writing to Kinetica on Writer.close");
			throw new ExecutionException(e);
		}
    }

  
    private GPUdb getGPUdb() {
    	GPUdbBase.Options gpudbOptions = new GPUdbBase.Options();

    	gpudbOptions.setUsername(spec.username());
    	gpudbOptions.setPassword(spec.password());
    		
    	GPUdb gpudb = null;
    	try {
    		gpudb = new GPUdb(spec.headNodeURL(), gpudbOptions);
    	} catch (GPUdbException e) {
    		LOG.error("Can't connect to GPUdb", e);
    		System.exit(99);
    	}
    	
    	return gpudb;
      }

  }

  @Override
  public Writer <T>createWriter(KineticaIO.Write<T> spec) {
    return new KineticaWriterImpl(spec);
  }
  

}
