package space.qbpo.utils;

import java.util.HashMap;
import java.util.List;

public class QbpoValueMap extends HashMap<String, List<String>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -990564118589657008L;
	
	public QbpoValueMap () {
		super ();
	}
	
	public QbpoValueMap (int initialCapacity) {
		super(initialCapacity);
	}
	
	public Integer getFirstInteger (String key) {
		if (!this.containsKey(key))
			return null;
		
		List<String> values = this.get(key);
		
		if (values == null || values.isEmpty())
			return null;
		
		String temp = values.get(0);
		Integer answer = null; 
		try {
			answer = Integer.parseInt(temp);
		} catch (NumberFormatException e) {
			return null;
		} 
		return answer;
		
	}
	
	public String getFirstString (String key) {
		if (!this.containsKey(key))
			return null; 
		
		List<String> values = this.get(key);
		
		if (values == null || values.isEmpty())
			return null; 
		
		String answer = values.get(0);
		
		return answer; 
	}
}
