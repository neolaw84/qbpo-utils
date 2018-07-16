package space.qbpo.utils.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.stereotype.Component;

public class DbMapReader extends JdbcCursorItemReader<Map<String, Object>> 
	implements ItemReader<Map<String, Object>>, InitializingBean {
	private static Logger log = LoggerFactory.getLogger(DbMapReader.class);

	DbMapReaderClient dbMapReaderClient;
	
	public DbMapReader setDbMapReaderClient (DbMapReaderClient dbMapReaderClient) {
		this.dbMapReaderClient = dbMapReaderClient;
		return this; 
	}
	
	private PreparedStatementSetter getNotNullPreparedStatementSetter (DbMapReaderClient 
			dbMapReaderClient) {
		PreparedStatementSetter answer = dbMapReaderClient.getPreparedStatementSetter();
		return answer != null ? answer : new PreparedStatementSetter() {
			
			@Override
			public void setValues(PreparedStatement arg0) throws SQLException {
				log.warn("PreparedStatementSetter is missing; doing nothing."); 
			}
		};
	}
	
    public void afterPropertiesSet() throws Exception {
    	String sql = dbMapReaderClient.getSql();
    	
        log.debug("SQL used is : " + sql);

        super.setSql(sql);
        
        PreparedStatementSetter preparedStatementSetter = getNotNullPreparedStatementSetter(
        		dbMapReaderClient);
        
        super.setPreparedStatementSetter(preparedStatementSetter);
        
        super.setDataSource(dbMapReaderClient.getDataSource());
        
        super.setRowMapper(new ColumnMapRowMapper());
        
        super.afterPropertiesSet();
    }

    public static interface DbMapReaderClient {
    	String getSql ();
    	PreparedStatementSetter getPreparedStatementSetter ();
    	DataSource getDataSource ();
    }
}
