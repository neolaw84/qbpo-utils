package space.qbpo.utils;

import java.util.List;

public interface QbpoUtil {
	public String getHelpMessage ();
	
	public void run (List<String> boolArgs, QbpoValueMap valueArgs);
	
	public String getCommand ();
}
