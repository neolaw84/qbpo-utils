package space.qbpo.utils.csv;

import org.slf4j.Logger;

public class LogProgress {
	
	int count = 0, mod = 100, modHit = 0;
	
	boolean chunkMode = false; 
	int chunkSize = 1;
	
	Logger log = null;
	
	public LogProgress (Logger log) {
		this.log = log;
		chunkMode = false;
		chunkSize = 1; 
	}
	
	public LogProgress (Logger log, Boolean chunkMode, int chunkSize) {
		this.log = log; 
		this.chunkMode = chunkMode; 
		this.chunkSize = chunkSize; 
	}
	
	public void progress () {
		count = count + 1;
		if (count % mod == 0) {
			int numRecords = count; 
			if (chunkMode) 
				numRecords = count * chunkSize; 
			
			log.info("Processed " + numRecords + " records.");
			modHit = modHit + 1;
			if (modHit == 9) {
				mod = mod * 10;
				modHit = 0;
			}
		}
	}
}