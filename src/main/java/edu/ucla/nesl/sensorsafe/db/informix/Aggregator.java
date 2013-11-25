package edu.ucla.nesl.sensorsafe.db.informix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.ucla.nesl.sensorsafe.model.Channel;

public class Aggregator {
	
	public static enum Type { AGGREGATE_BY, AGGREGATE_RANGE };
	
	private static Map<Type, String[]> aggregatorNameMap = new HashMap<Type, String[]>();
	private static String NOISY_PREFIX = "Noisy"; 
	
	static {
		aggregatorNameMap.put(Type.AGGREGATE_BY, new String[] { "AggregateBy", "DownSample" });
		aggregatorNameMap.put(Type.AGGREGATE_RANGE, new String[] { "AggregateRange", "Calculate" });
	}

	public Type aggregator;
	public String sqlExpression;
	public String calendar;
	public List<Channel> channels;
	public List<Channel> targetStreamChannels;
	public boolean isNoisy = false;
	public boolean isAvgAggregator = false;
	public double epsilon = Double.NaN;
	
	public Aggregator(String expr, List<Channel> channels) {
		expr = expr.trim();
		if (expr.charAt(expr.length()-1) != ')') {
			throw new IllegalArgumentException(ExceptionMessages.MSG_INVALID_AGGREGATOR_EXPRESSION);
		}

		// Find out aggregator type.
		if (expr.toLowerCase().startsWith(NOISY_PREFIX.toLowerCase())) {
			isNoisy = true;
			expr = expr.replace(NOISY_PREFIX, "");
		}
		
		for (Entry<Type, String[]> entry: aggregatorNameMap.entrySet()) {
			for (String name: entry.getValue()) {
				if (expr.toLowerCase().startsWith(name.toLowerCase() + "(")) {
					aggregator = entry.getKey();
					expr = expr.toLowerCase().replace(name.toLowerCase() + "(", "");
					expr = expr.substring(0, expr.length() - 1);
				}
			}
		}
		
		if (aggregator == null) {
			throw new IllegalArgumentException(ExceptionMessages.MSG_INVALID_AGGREGATOR_EXPRESSION);
		}
		
		// Extract aggregate arguments and properly split.
		String[] argstemp = expr.split(",");
		List<String> arguments = new ArrayList<String>();
		boolean withinQuote = false;
		for (int i = 0; i < argstemp.length; i++) {
			argstemp[i] = argstemp[i].trim();
			if (argstemp[i].charAt(0) == '"') {
				withinQuote = true;
				argstemp[i] = argstemp[i].replace("\"", "");
				arguments.add(argstemp[i]);
				continue;
			}
			if (withinQuote) {
				if (argstemp[i].charAt(argstemp[i].length()-1) == '"' ) {
					withinQuote = false;
					argstemp[i] = argstemp[i].replace("\"", "");
				}
				arguments.set(arguments.size()-1, arguments.get(arguments.size()-1) + "," + argstemp[i]);				
			} else {
				arguments.add(argstemp[i]);
			}
		}
		
		// Check argument validity
		if (isNoisy) {
			if (aggregator == Aggregator.Type.AGGREGATE_BY && arguments.size() == 3) {
				sqlExpression = arguments.get(0);
				calendar = determineCalendar(arguments.get(1));
				epsilon = Double.valueOf(arguments.get(2));
			} else if (aggregator == Aggregator.Type.AGGREGATE_RANGE && arguments.size() == 2) {
				sqlExpression = arguments.get(0);
				epsilon = Double.valueOf(arguments.get(1));
			} else {
				throw new IllegalArgumentException(ExceptionMessages.MSG_INVALID_NUM_AGGREGATOR_ARGUMENTS);
			}
		} else {
			if (aggregator == Aggregator.Type.AGGREGATE_BY && arguments.size() == 2) {
				sqlExpression = arguments.get(0);
				calendar = determineCalendar(arguments.get(1));
			} else if (aggregator == Aggregator.Type.AGGREGATE_RANGE && arguments.size() == 1) {
				sqlExpression = arguments.get(0);
			} else {
				throw new IllegalArgumentException(ExceptionMessages.MSG_INVALID_NUM_AGGREGATOR_ARGUMENTS);
			}
		}
		
		// determine if avg aggregator
		if (sqlExpression.toLowerCase().contains("avg")) {
			isAvgAggregator = true;
		} else {
			isAvgAggregator = false;
		}
		
		// Set aggregate channel names and convert sql expression channel names.
		String oriSqlExpression = sqlExpression;
		targetStreamChannels = channels;
		
		convertSqlExprChannelNames();
		
		setAggregateChannels(oriSqlExpression);
	}

	private void convertSqlExprChannelNames() {
		Map<String, String> map = new HashMap<String, String>();
		int idx = 1;
		for (Channel ch: targetStreamChannels) {
			map.put("$" + ch.name, "$channel" + idx);
			idx += 1;
		}
		for (Map.Entry<String, String> item: map.entrySet()) {
			sqlExpression = sqlExpression.replace(item.getKey(), item.getValue());
		}
	}

	private String determineCalendar(String aggregatorOption) {
		if (aggregatorOption.equalsIgnoreCase("1min") 
				|| aggregatorOption.equalsIgnoreCase("min") 
				|| aggregatorOption.equalsIgnoreCase("minute") 
				|| aggregatorOption.equalsIgnoreCase("1minute") 
				|| aggregatorOption.equalsIgnoreCase("mins") 
				|| aggregatorOption.equalsIgnoreCase("minutes")) {
			return "ts_1min";
		} else if (aggregatorOption.equalsIgnoreCase("15min")
				|| aggregatorOption.equalsIgnoreCase("15mins")
				|| aggregatorOption.equalsIgnoreCase("15minutes")
				|| aggregatorOption.equalsIgnoreCase("15minute")) {
			return "ts_15min";
		} else if (aggregatorOption.equalsIgnoreCase("30min")
				|| aggregatorOption.equalsIgnoreCase("30mins")
				|| aggregatorOption.equalsIgnoreCase("30minutes")
				|| aggregatorOption.equalsIgnoreCase("30minute")) {
			return "ts_30min";
		} else if (aggregatorOption.equalsIgnoreCase("1hour") 
				|| aggregatorOption.equalsIgnoreCase("hour")
				|| aggregatorOption.equalsIgnoreCase("hours")) {
			return "ts_1hour";
		} else if (aggregatorOption.equalsIgnoreCase("1day") 
				|| aggregatorOption.equalsIgnoreCase("day")
				|| aggregatorOption.equalsIgnoreCase("days")) {
			return "ts_1day";
		} else if (aggregatorOption.equalsIgnoreCase("1week")
				|| aggregatorOption.equalsIgnoreCase("week")
				|| aggregatorOption.equalsIgnoreCase("weeks")) {
			return "ts_1week";
		} else if (aggregatorOption.equalsIgnoreCase("1month") 
				|| aggregatorOption.equalsIgnoreCase("month")
				|| aggregatorOption.equalsIgnoreCase("months")) {
			return "ts_1month";
		} else if (aggregatorOption.equalsIgnoreCase("1year") 
				|| aggregatorOption.equalsIgnoreCase("year")
				|| aggregatorOption.equalsIgnoreCase("years")) {
			return "ts_1year";
		} else {
			throw new IllegalArgumentException(ExceptionMessages.MSG_INVALID_CALENDAR_TYPE);
		}
	}
	
	public static boolean isAggregateExpression(String expr) {
		expr = expr.trim();

		// Find out aggregator type.
		if (expr.toLowerCase().startsWith(NOISY_PREFIX.toLowerCase())) {
			expr = expr.replace(NOISY_PREFIX, "");
		}

		if (expr.charAt(expr.length()-1) != ')') {
			return false;
		}

		for (Entry<Type, String[]> entry: aggregatorNameMap.entrySet()) {
			for (String name: entry.getValue()) {
				if (expr.toLowerCase().startsWith(name.toLowerCase() + "(")) {
					return true;
				}
			}
		}
		return false;
	}
	
	private void setAggregateChannels(String oriSqlExpression) {
		String[] aggExprChannels = oriSqlExpression.split(",");
		channels = new ArrayList<Channel>();
		Pattern columnPattern = Pattern.compile("\\$channel[0-9]+");
		Pattern integerPattern = Pattern.compile("[0-9]+");
		Matcher columnMatcher = columnPattern.matcher(sqlExpression);
		int i = 0;
		while (columnMatcher.find()) {
			String column = columnMatcher.group();
			Matcher integerMatcher = integerPattern.matcher(column);
			while (integerMatcher.find()) {
				int channelNum = Integer.valueOf(integerMatcher.group());
				if (!targetStreamChannels.get(channelNum-1).type.equals("float")
						&& !targetStreamChannels.get(channelNum-1).type.equals("int")) {
					throw new IllegalArgumentException("Aggregate on non-numeric data type is not allowed.");
				}
				channels.add(new Channel(
						aggExprChannels[i++], 
						"float"
				));
			}
		}
		if (channels.size() <= 0) {
			throw new IllegalArgumentException("Invalid aggregate expression.");
		}
	}

	public boolean isNoisy() {
		return isNoisy;
	}
}
