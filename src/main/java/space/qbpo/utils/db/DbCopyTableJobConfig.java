package space.qbpo.utils.db;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import space.qbpo.utils.csv.LogProgress;

@Configuration
public class DbCopyTableJobConfig {
	
	private static final Logger log = LoggerFactory.getLogger(DbCopyTableJobConfig.class);
	
	private static final String MAP_KEY_TO_LOWER_CASE_PROCESSOR = "mapKeyToLowerCaseProcessor";

	private static final String DB_COPY_TABLE_CHUNK_LISTENER = "dbCopyTableChunkListener";
	
	@Value ("${qbpo.utils.skiplimit:128}")
	Integer skipLimit; 
	
	@Value ("${qbpo.utils.chunksize:16}")
	Integer chunkSize; 
	
	@Bean @Autowired @Qualifier (DbCopyTableConfig.DB_COPY_TABLE_JOB)
	public Job dbCopyTableJob (JobBuilderFactory jobBuilderFactory, 
			@Qualifier (DbCopyTableConfig.DB_COPY_TABLE_STEP) Step dbCopyTableStep) {
		return jobBuilderFactory.get(DbCopyTableConfig.DB_COPY_TABLE_JOB)
				.start(dbCopyTableStep)
				.build(); 
	}

	@Bean @Qualifier (DbCopyTableConfig.DB_COPY_TABLE_STEP) @Autowired 
	public Step dbCopyTableStep (StepBuilderFactory stepBuilderFactory, 
			@Qualifier (DbCopyTableConfig.DB_COPY_TABLE_SOURCE_DB_MAP_READER)	DbMapReader dbMapReader,
			@Qualifier (DbCopyTableConfig.DB_COPY_TABLE_DESTINATION_DB_MAP_WRITER) DbMapWriter dbMapWriter, 
			@Qualifier (DB_COPY_TABLE_CHUNK_LISTENER) ChunkListener chunkListener) {
		StepBuilder stepBuilder = stepBuilderFactory.get(DbCopyTableConfig.DB_COPY_TABLE_STEP);

		return stepBuilder.<Map<String, Object>, Map<String, Object>>chunk (chunkSize)
				.reader(dbMapReader)
				.processor(new PassThroughItemProcessor<>())
				.writer(dbMapWriter)
				.faultTolerant()
				.skipLimit(skipLimit)
				.skip(Exception.class)
				.listener(chunkListener)
				.build();
	}	
	
	@Bean @Qualifier (DB_COPY_TABLE_CHUNK_LISTENER) 
	public ChunkListener dbCopyTableChunkListener () {
		return new ChunkListener() {
			
			final LogProgress logProgress = new LogProgress(log, true, chunkSize);
			
			@Override
			public void beforeChunk(ChunkContext arg0) {
				// do nothing;
			}
			
			@Override
			public void afterChunkError(ChunkContext arg0) {
				// TODO Auto-generated method stub
				log.error ("A chunk error has occured.");
			}
			
			@Override
			public void afterChunk(ChunkContext arg0) {
				logProgress.progress();
			}
		};
	}
	
	@Bean @Qualifier (MAP_KEY_TO_LOWER_CASE_PROCESSOR) 
	public ItemProcessor<Map<String, Object>, Map<String, Object>> mapKeyToLowerCaseProcessor () {
		return new ItemProcessor<Map<String,Object>, Map<String,Object>>() {

			@Override
			public Map<String, Object> process(Map<String, Object> item) throws Exception {
				Map<String, Object> answer = new LinkedHashMap<>();
				for (Map.Entry<String, Object> e : item.entrySet()) {
					String key = e.getKey();
					Object value = e.getValue();
					if (key != null)
						answer.put(key.toLowerCase(), value);
				}
				return answer;
			}
		};
	}
}
