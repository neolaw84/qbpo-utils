package space.qbpo.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * This is the configuration to be imported when you use
 * this project as a library.
 * 
 * @author edward
 * 
 */
@Configuration
@Import (value = {
		QbpoUtilHelp.class, 
		QbpoUtilsApplicationRunner.class
		})

public class QbpoUtilsConfig {
	private static Logger log = LoggerFactory.getLogger(QbpoUtilsConfig.class);
	
	@Value ("${space.qbpo.application.enable:true}")
	Boolean enabled;
	
	public Boolean getEnabled() {
		return enabled;
	}

	public void setEnabled(Boolean enabled) {
		this.enabled = enabled;
	}

	List<QbpoUtil> qbpoUtils;
	Map<String, QbpoUtil> command2QbpoUtil;
	
	@Autowired public void setQbpoUtils (List<QbpoUtil> qbpoUtils) {
		if (qbpoUtils.isEmpty())
			log.warn("QbpoUtils are empty.");
		this.qbpoUtils = qbpoUtils;
		
		command2QbpoUtil = new HashMap<>(64);
		for (QbpoUtil qbpoUtil : qbpoUtils) {
			String command = qbpoUtil.getCommand();
			command2QbpoUtil.put(command, qbpoUtil);
		}
	}
	
	public QbpoUtil getQbpoUtilByCommand (String command) {
		return command2QbpoUtil.get(command);
	}
}
