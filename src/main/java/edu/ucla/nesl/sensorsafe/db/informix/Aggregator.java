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
	public static Map<Type, String[]> aggregatorNameMap = new HashMap<Type, String[]>();

	static {
		aggregatorNameMap.put(Type.AGGREGATE_BY, new String[] { "AggregateBy", "DownSample" });
		aggregatorNameMap.put(Type.AGGREGATE_RANGE, new String[] { "AggregateRange", "Calculate" });
	}

	public Type aggregator;
	public String sqlExpression;
	public String oriSqlExpression;
	public String calendar;
	public List<Channel> targetStreamChannels;

	public Aggregator(String expr, List<Channel> channels) {
		expr = expr.trim();
		if (expr.charAt(expr.length()-1) != ')') {
			throw new IllegalArgumentException(ExceptionMessages.MSG_INVALID_AGGREGATOR_EXPRESSION);
		}

		// Find out aggregator type.
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
		
		// Get arguments.
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
		if (aggregator == Aggregator.Type.AGGREGATE_BY && arguments.size() == 2) {
			sqlExpression = arguments.get(0);
			calendar = determineCalendar(arguments.get(1));
		} else if (aggregator == Aggregator.Type.AGGREGATE_RANGE && arguments.size() == 1) {
			sqlExpression = arguments.get(0);
		} else {
			throw new IllegalArgumentException(ExceptionMessages.MSG_INVALID_AGGREGATOR_EXPRESSION);
		}
		
		oriSqlExpression = sqlExpression;
		targetStreamChannels = channels;
		
		convertSqlExprChannelNames();
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
	
	public List<Channel> getAggregateChannels() {
		String[] aggExprChannels = oriSqlExpression.split(",");
		List<Channel> aggChannels = new ArrayList<Channel>();
		Pattern columnPattern = Pattern.compile("\\$channel[0-9]+");
		Pattern integerPattern = Pattern.compile("[0-9]+");
		Matcher columnMatcher = columnPattern.matcher(sqlExpression);
		int i = 0;
		while (columnMatcher.find()) {
			String column = columnMatcher.group();
			Matcher integerMatcher = integerPattern.matcher(column);
			while (integerMatcher.find()) {
				int channelNum = Integer.valueOf(integerMatcher.group());
				aggChannels.add(new Channel(aggExprChannels[i++], targetStreamChannels.get(channelNum-1).type));
			}
		}
		
		if (aggChannels.size() <= 0) {
			throw new IllegalArgumentException("Invalid aggregate expression.");
		}
		
		return aggChannels;
	}
}
