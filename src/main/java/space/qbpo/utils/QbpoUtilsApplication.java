package space.qbpo.utils;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
/**
 * The application to demonstrate running. 
 * 
 * @author edward
 *
 */
@SpringBootApplication
@EnableConfigurationProperties
public class QbpoUtilsApplication {

	public static void main(String[] args) {
		SpringApplication.run(QbpoUtilsApplication.class, args);
	}
	
	
}
