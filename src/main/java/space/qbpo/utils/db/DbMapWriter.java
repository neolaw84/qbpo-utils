package space.qbpo.utils.db;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.support.ColumnMapItemPreparedStatementSetter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

public class DbMapWriter extends JdbcBatchItemWriter <Map<String, Object>>
implements ItemWriter<Map<String, Object>>, InitializingBean {
	
	DbMapWriterClient dbMapWriterClient;

	public DbMapWriterClient getDbMapWriterClient() {
		return dbMapWriterClient;
	}

	public DbMapWriter setDbMapWriterClient(DbMapWriterClient dbMapWriterClient) {
		this.dbMapWriterClient = dbMapWriterClient;
		return this;
	}

	@Override
	public void afterPropertiesSet () {
		DataSource dataSource = dbMapWriterClient.getDataSource();
		String tableName = dbMapWriterClient.getTableName();
		
		String insertSql = getInsertSql (tableName, dataSource); 
		        
		super.setDataSource(dataSource);
		super.setItemPreparedStatementSetter(new ColumnMapItemPreparedStatementSetter());
		super.setSql(insertSql);
		super.afterPropertiesSet();
	}
	
	private String getInsertSql (String tableName, DataSource dataSource) {
		JdbcTemplate jdbcTemplate = new JdbcTemplate (dataSource);
		
		jdbcTemplate.afterPropertiesSet();
		
		String selectSql = "SELECt * FROM " + tableName; 
		
		List<String> columns = jdbcTemplate.query(selectSql, new ResultSetExtractor<List<String>>() {

			@Override
			public List<String> extractData(ResultSet arg0) 
					throws SQLException, DataAccessException {
				ResultSetMetaData resultSetMetaData = arg0.getMetaData();
				int numColumns = resultSetMetaData.getColumnCount();
				List<String> columns = new ArrayList<>(numColumns);
				for (int i = 0; i < numColumns; i = i + 1) {
					String column = resultSetMetaData.getColumnLabel(i + 1);
					columns.add(column);
				}
				return columns; 
			}
			
		});

        String allColumns = StringUtils.join(columns, ",");
        String allValues = StringUtils.repeat("?", ",", columns.size());

		String insertSql = new StringBuilder("INSERT INTO ").append(tableName)
				.append(" ( ").append(allColumns).append(" ) ")
				.append(" VALUES ( ").append(allValues).append(" ) ").toString(); 

		return insertSql;
	}

	public static interface DbMapWriterClient {
		String getTableName ();
		DataSource getDataSource ();
	}
}
