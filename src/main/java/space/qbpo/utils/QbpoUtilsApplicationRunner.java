package space.qbpo.utils;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.stereotype.Component;

/**
 * The ApplicationRunner to help run the Utils based on 
 * commandline arguments. 
 * 
 * @author edward
 *
 * Make sure you import QbpoUtilsConfig if you use this 
 * project as libary to ensure this class is found by 
 * the ComponentScan of Spring. 
 *
 */
@Component
public class QbpoUtilsApplicationRunner implements ApplicationRunner {

	public static void main(String[] args) {
		SpringApplication.run(QbpoUtilsApplication.class, args);
	}
	
	private static Logger log = LoggerFactory.getLogger(QbpoUtilsApplication.class);
	
	@Autowired QbpoUtilsConfig qbpoUtilsConfig;
	
	@Override
	public void run(ApplicationArguments args) throws Exception {
		if (!qbpoUtilsConfig.getEnabled()) {
			log.debug("Qbpo Utils are not enabled ... quitting.");
			return;
		}
		
		List<String> nonOptionArgs = args.getNonOptionArgs();
		Set<String> optionNames = args.getOptionNames();
		
		log.debug("List of nonOptionArgs : ");
		for (String arg : nonOptionArgs) {
			log.debug(arg);
		}
		
		QbpoValueMap valueArgs = new QbpoValueMap(64);
		log.debug("List of optionArgs : ");
		for (String name : optionNames) {
			List<String> values = args.getOptionValues(name);
			valueArgs.put(name, values);
			
			log.debug(name + " --> ");
			for (String value : values) {
				log.debug("\t" + value);
			}
		}
		
		String command = "help";
		if (!nonOptionArgs.isEmpty()) {
			command = nonOptionArgs.get(0);
			nonOptionArgs = nonOptionArgs.subList(1, nonOptionArgs.size());
		}
		
		QbpoUtil qbpoUtil = qbpoUtilsConfig.getQbpoUtilByCommand(command);
		if (qbpoUtil == null) {
			log.error("Command not found.");
			qbpoUtil = qbpoUtilsConfig.getQbpoUtilByCommand("help");
			qbpoUtil.run(nonOptionArgs, valueArgs);
		}
	}
}

