package space.qbpo.utils;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class QbpoUtilHelp implements QbpoUtil {
	private Logger log = LoggerFactory.getLogger(QbpoUtilHelp.class);
	
	private static final String HELP_COMMAND = "help";
	
	QbpoUtilsConfig qbpoUtilsConfig;
	
	@Autowired public void setQbpoUtilsConfig (QbpoUtilsConfig qbpoUtilsConfig) {
		this.qbpoUtilsConfig = qbpoUtilsConfig;
	}

	@Override
	public String getHelpMessage() {
		List<QbpoUtil> utils = qbpoUtilsConfig.qbpoUtils;
		
		StringBuilder sb = new StringBuilder(System.lineSeparator())
				.append("Usage : java -jar this.jar <command> ")
				.append("<option1> <option2> ... ")
				.append("--<parameter1>=<value1> --<parameter2>=<value2> ... ")
				.append(System.lineSeparator())
				.append("Available commands are : ")
				.append(System.lineSeparator());
		
		for (QbpoUtil util : utils) {
			sb.append(util.getCommand()).append(System.lineSeparator());
		}
		
		sb.append("Usage of each commands are : ");
		for (Map.Entry<String, QbpoUtil> e : qbpoUtilsConfig.command2QbpoUtil.entrySet()) {
			sb.append("java -jar this.jar ").append(e.getKey());
			if (!HELP_COMMAND.equals(e.getKey())) {
				sb.append(e.getValue().getHelpMessage());
			}
			sb.append(System.lineSeparator());
		}
				
		return sb.toString();
	}

	@Override
	public void run(List<String> boolArgs, QbpoValueMap valueArgs) {
		log.info(getHelpMessage());
	}

	@Override
	public String getCommand() {
		return HELP_COMMAND;
	}
}
