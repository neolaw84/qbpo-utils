package space.qbpo.utils.db;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import space.qbpo.utils.QbpoUtil;
import space.qbpo.utils.QbpoValueMap;

@Component
public class DbCopyTable implements QbpoUtil {

	private static Logger log = LoggerFactory.getLogger(DbCopyTable.class);

	static final String SOURCE = "source";
	static final String DESTINATION = "destination";
	static final String JOB_PARAMETERS_SOURCE = "#{jobParameters['" + SOURCE + "']}";
	static final String JOB_PARAMETERS_DESTINATION = "#{jobParameters['" + DESTINATION + "']}";
	
	@Override
	public String getHelpMessage() {
		StringBuilder sb = new StringBuilder(" --" + SOURCE + "=source_table ")
				.append("--" + DESTINATION + "=destination_table)")
				.append(System.lineSeparator())
				.append("This command copies contents of the source_table ")
				.append(System.lineSeparator())
				.append("into destination_table. Notice that the source and ")
				.append(System.lineSeparator())
				.append("destination should be of the same sets of columns (.")
				.append(System.lineSeparator())
				.append("regardless of order).");
		
		return sb.toString();
	}

	@Autowired JobLauncher jobLauncher;

	@Autowired @Qualifier (DbCopyTableConfig.DB_COPY_TABLE_JOB) Job job;
	
	@Override
	public void run(List<String> boolArgs, QbpoValueMap valueArgs) {
		JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
		JobParameters jobParameters = jobParametersBuilder
			.addLong("ts", System.currentTimeMillis())
			.addString(SOURCE, valueArgs.getFirstString(SOURCE))
			.addString(DESTINATION, valueArgs.getFirstString(DESTINATION))
			.toJobParameters();
		try {
			jobLauncher.run(job, jobParameters);
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getCommand() {
		return "db-copy-table";
	}
	
	
}
