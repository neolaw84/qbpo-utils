package space.qbpo.utils.csv;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import space.qbpo.utils.QbpoUtil;
import space.qbpo.utils.QbpoValueMap;

@Component
public class CsvLabelEncoder implements QbpoUtil {

	private static final Logger log = LoggerFactory.getLogger(CsvLabelEncoder.class);
	private static final String HASH_CSV_COLUMNS = "csv-label-encoder";

	@Override
	public String getHelpMessage() {
		return new StringBuilder(" --str-col=string-col-1 --str-col=string-col-2 ")
				.append("--bag-col=set-col-1 --bag-col-sep=set-col-sep" )
				.append("--in=/path/to/input/file --out=/path/to/output/file")
				.append(System.lineSeparator())
				.append("This command label-encode (with simple integer sequence) the given columns retaining ")
				.append(System.lineSeparator())
				.append("the relational integrity of the data. ")
				.append(System.lineSeparator())
				.append("This does not do the encryption hash.")
				.append(System.lineSeparator())
				.append("Bag columns are actually list-columns in nature.")
				.append(System.lineSeparator())
				.append("in other words, [A|A|B] and [A|B] are considered different.")
				.toString();
	}

	private static final String SET_COL = "bag-col";
	private static final String STR_COL = "str-col";
	private static final String SET_COL_SEP = "bag-col-sep";

	private static final String IN = "in";
	private static final String OUT = "out";

	private List<String> getDefaultSetColSeps () {
		List<String> answer = new ArrayList<>(1);
		answer.add("|");
		return answer; 
	}

	@Override
	public void run(List<String> boolArgs, QbpoValueMap valueArgs) {
		Map<String, Map<String, Integer>> stringCols2HashMap = getStringCols (valueArgs);
		Map<String, Map<String, Integer>> setCols2HashMap = getSetCols (valueArgs);
		List<String> setColSeps = valueArgs.getOrDefault(SET_COL_SEP, getDefaultSetColSeps());

		String inPath = valueArgs.getFirstString(IN);
		String outPath = valueArgs.getFirstString(OUT);

		log.info(new StringBuilder(System.lineSeparator())
				.append("Starting the csv-label-encoder ... ")
				.append(System.lineSeparator())
				.append("in : ").append(inPath).append(System.lineSeparator())
				.append("out : ").append(outPath).append(System.lineSeparator())
				.toString());



		process (inPath, outPath, stringCols2HashMap, setCols2HashMap, setColSeps);
	}

	private void process(String inPath, String outPath, Map<String, Map<String, Integer>> stringCols2HashMap,
			Map<String, Map<String, Integer>> setCols2HashMap, List<String> setColSeps) {
		try (
			CSVParser parser = CSVParser.parse(
					new InputStreamReader(new FileInputStream(inPath)), 
					CSVFormat.EXCEL.withFirstRecordAsHeader());
			CSVPrinter printer = new CSVPrinter(
					new OutputStreamWriter (new FileOutputStream(outPath)), CSVFormat.EXCEL);) {
			
			Map<String, Integer> columnName2Index = parser.getHeaderMap();
			
			List<String> header = getHeader (columnName2Index);
			printer.printRecord(header);
			
			warnColNameMismatch (stringCols2HashMap, columnName2Index);
			warnColNameMismatch (setCols2HashMap, columnName2Index);
			
			String setColSepStr = StringUtils.join(setColSeps);
		
			LogProgress logProgress = new LogProgress(log);
			for (CSVRecord record : parser) {
				process(record, printer, stringCols2HashMap, setCols2HashMap, setColSepStr, header);
				logProgress.progress();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


	private List<String> getHeader(Map<String, Integer> columnName2Index) throws IOException {
		List<String> header = new ArrayList<>(columnName2Index.size());
		for (int i = 0; i < columnName2Index.size(); i = i + 1)
			header.add("");
		for (Map.Entry<String, Integer> e : columnName2Index.entrySet()) {
			header.set(e.getValue(), e.getKey());
		}
		//printer.printRecord(header);
		return header;
	}

	private void process(CSVRecord record, CSVPrinter printer, Map<String, Map<String, Integer>> stringCols2HashMap,
			Map<String, Map<String, Integer>> setCols2HashMap, String setColSepStr, List<String> header) throws IOException {
		List<String> toOutput = new ArrayList<>(record.size());
		for (String col : header) {
			if (!stringCols2HashMap.containsKey(col)
					&& !setCols2HashMap.containsKey(col)) {
				toOutput.add(record.get(col));
			} else if (stringCols2HashMap.containsKey(col)) {
				toOutput.add(getHashValue (record.get(col), stringCols2HashMap.get(col)));
			} else if (setCols2HashMap.containsKey(col)) {
				toOutput.add(getHashValueForSet (record.get(col), setCols2HashMap.get(col), setColSepStr));
			}
		}
		printer.printRecord(toOutput);
	}

	private String getHashValueForSet(String string, Map<String, Integer> hashMap,
			String setColSepStr) {
		String[] setVals = StringUtils.split(string, setColSepStr);
		List<String> setValList = new ArrayList<>(Arrays.asList(setVals));
		Collections.sort(setValList);
		String sortedString = StringUtils.join(setValList);
		
		return getHashValue (sortedString, hashMap);
	}

	private String getHashValue(String string, Map<String, Integer> hashMap) {
		Integer answerInt = -1;
		if (hashMap.containsKey(string)) {
			answerInt = hashMap.get(string);
		} else {
			answerInt = hashMap.size();
			hashMap.put(string, answerInt);
		}
		return StringUtils.leftPad(answerInt.toString(), 10, '0');
	}

	private void warnColNameMismatch(Map<String, Map<String, Integer>> stringCols2HashMap,
			Map<String, Integer> columnName2Index) {
		for (String col : stringCols2HashMap.keySet()) {
			if (!columnName2Index.containsKey(col)) {
				log.warn("Column name not found in in-file : " + col);
			}
		}
		
	}

	private Map<String, Map<String, Integer>> getSetCols(QbpoValueMap valueArgs) {
		return initMapOfString (valueArgs, SET_COL);
	}

	private Map<String, Map<String, Integer>> getStringCols (QbpoValueMap valueArgs) {
		return initMapOfString (valueArgs, STR_COL);
	}

	private Map<String, Map<String, Integer>> initMapOfString (QbpoValueMap valueArgs, String key) {
		List<String> colList = valueArgs.getOrDefault(key, new ArrayList<>(1));

		Map<String, Map<String, Integer>> answer = new HashMap<>(colList.size());
		for (String col : colList) {
			answer.put(col, new HashMap<>(1024 * 1024));
		}
		return answer;
	}

	@Override
	public String getCommand() {
		return HASH_CSV_COLUMNS;
	}

}
