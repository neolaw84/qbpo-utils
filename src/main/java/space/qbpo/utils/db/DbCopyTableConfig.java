package space.qbpo.utils.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.PreparedStatementSetter;

import space.qbpo.utils.db.DbMapReader.DbMapReaderClient;
import space.qbpo.utils.db.DbMapWriter.DbMapWriterClient;

@Configuration
@JobScope
public class DbCopyTableConfig {

	private static final Logger log = LoggerFactory.getLogger(DbCopyTableConfig.class);

	private static final String SOURCE_DATASOURCE_PROPERTIES = "sourceDataSourceProperties";
	static final String SOURCE_DATASOURCE = "sourceDataSource";

	static final String DESTINATION_DATASOURCE_PROPERTIES = "destinationDataSourceProperties";
	private static final String DESTINATION_DATASOURCE = "destinationDataSource";

	static final String DB_COPY_TABLE_JOB = "dbCopyTableJob";
	static final String DB_COPY_TABLE_STEP = "dbCopyTableStep";
	
	static final String DB_COPY_TABLE_SOURCE_DB_MAP_READER = 
			"dbCopyTableSourceDbMapReader";
	
	static final String DB_COPY_TABLE_DESTINATION_DB_MAP_WRITER = 
			"dbCopyTableDestinationDbMapReader";
	
	//@Value ("${qbpo.utils.source.table:my_table_2}")
	@Value (DbCopyTable.JOB_PARAMETERS_SOURCE)
	String sourceTable;
	
	//@Value ("${qbpo.utils.destination.table:my_table_2}")
	@Value (DbCopyTable.JOB_PARAMETERS_DESTINATION)
	String destinationTable;
	
	@Bean (destroyMethod = "") @Qualifier (DB_COPY_TABLE_SOURCE_DB_MAP_READER) 
	@Autowired @JobScope
	public DbMapReader dbMapReader (DbMapReaderClient dbMapReaderClient) {
		DbMapReader dbMapReader = new DbMapReader().setDbMapReaderClient(dbMapReaderClient);
			
		return dbMapReader;
	}
	
	@Bean @Autowired @JobScope
	public DbMapReaderClient dbMapReaderClient (
			@Qualifier (SOURCE_DATASOURCE) DataSource dataSource) {
		DbMapReaderClient dbMapReaderClient = new DbMapReaderClient() {
			
			@Override
			public String getSql() {
				return "SELECT * FROM " + sourceTable ;
			}
			
			@Override
			public PreparedStatementSetter getPreparedStatementSetter() {
				return new PreparedStatementSetter() {
					
					@Override
					public void setValues(PreparedStatement arg0) throws SQLException {
						// do nothing;
					}
				};
			}
			
			@Override
			public DataSource getDataSource() {
				return dataSource;
			}
		};
		
		
		return dbMapReaderClient;
	} 
	
	@Bean @Qualifier (SOURCE_DATASOURCE_PROPERTIES) @JobScope
	@ConfigurationProperties (prefix = "qbpo.utils.source.ds")
	public DataSourceProperties sourceDataSourceProperites () {
		return new DataSourceProperties();
	}

	@Bean @Qualifier (SOURCE_DATASOURCE) @Autowired	 @JobScope
	public DataSource dss(@Qualifier (SOURCE_DATASOURCE_PROPERTIES) 
	DataSourceProperties dataSourceProperties) {
		try {
			DataSource answer = dataSourceProperties.initializeDataSourceBuilder()
					.build();
			return answer; 
		} catch (Exception e) { e.printStackTrace(); return null; }
	}

	@Bean @Qualifier (DB_COPY_TABLE_DESTINATION_DB_MAP_WRITER) @Autowired  @JobScope
	public DbMapWriter dbMapWriter (@Qualifier (DESTINATION_DATASOURCE) DataSource dataSource) {
		DbMapWriter dbMapWriter = new DbMapWriter().setDbMapWriterClient(new DbMapWriterClient() {
			
			@Override
			public String getTableName() {
				return destinationTable;
			}
			
			@Override
			public DataSource getDataSource() {
				return dataSource;
			}
		});
		// dbMapWriter.afterPropertiesSet();
		return dbMapWriter;
	}
	
	@Bean @Qualifier (DESTINATION_DATASOURCE_PROPERTIES) @JobScope
	@ConfigurationProperties (prefix = "qbpo.utils.destination.ds") 
	public DataSourceProperties destinationDataSourceProperties () {
		return new DataSourceProperties();
	}
	
	@Bean @Qualifier(DESTINATION_DATASOURCE) @Autowired	 @JobScope
	public DataSource dsd(@Qualifier (DESTINATION_DATASOURCE_PROPERTIES)
		DataSourceProperties dataSourceProperties) {
		try {
			return dataSourceProperties.initializeDataSourceBuilder()
					.build();
		} catch (Exception e) { return null; }
	}
}
