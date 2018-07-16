package space.qbpo.utils.db;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class InMemoryDbConfig {
	
	@ConfigurationProperties (prefix = "spring.datasource")
	@Bean @Primary
	public DataSourceProperties dataSourceProperties () {
		return new DataSourceProperties();
	}
	
	@Bean @Autowired @Primary 
    public DataSource dataSource (DataSourceProperties dataSourceProperties) {
    	try {
    		DataSource dataSource = dataSourceProperties.initializeDataSourceBuilder().build();
    		return dataSource;
    	} catch (Exception e) {
    		e.printStackTrace();
    		return null; 
    	}
    }

}
