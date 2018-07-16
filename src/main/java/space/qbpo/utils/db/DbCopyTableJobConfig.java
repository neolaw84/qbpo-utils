package space.qbpo.utils.db;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DbCopyTableJobConfig {
	
	private static final String MAP_KEY_TO_LOWER_CASE_PROCESSOR = "mapKeyToLowerCaseProcessor";
	
	@Value ("${qbpo.utils.skiplimit:1024}")
	Integer skipLimit; 
	
	@Value ("${qbpo.utils.chunksize:256}")
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
			@Qualifier (DbCopyTableConfig.DB_COPY_TABLE_DESTINATION_DB_MAP_WRITER) DbMapWriter dbMapWriter) {
		StepBuilder stepBuilder = stepBuilderFactory.get(DbCopyTableConfig.DB_COPY_TABLE_STEP);

		return stepBuilder.<Map<String, Object>, Map<String, Object>>chunk (chunkSize)
				.reader(dbMapReader)
				.processor(new PassThroughItemProcessor<>())
				.writer(dbMapWriter)
				.faultTolerant()
				.skipLimit(skipLimit)
				.skip(Exception.class)
				.build();
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
