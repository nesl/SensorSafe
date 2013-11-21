package edu.ucla.nesl.sensorsafe.db.informix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.ucla.nesl.sensorsafe.tools.Log;

public class Aggregator {
	public static enum Type { AGGREGATE_BY, AGGREGATE_RANGE };
	public static Map<Type, String[]> aggregatorNameMap = new HashMap<Type, String[]>();

	static {
		aggregatorNameMap.put(Type.AGGREGATE_BY, new String[] { "AggregateBy", "DownSample" });
		aggregatorNameMap.put(Type.AGGREGATE_RANGE, new String[] { "AggregateRange", "Calculate" });
	}

	public Type type;
	public List<String> arguments;

	public Aggregator(String expr) {
		expr = expr.trim();
		if (expr.charAt(expr.length()-1) != ')') {
			throw new IllegalArgumentException(ExceptionMessages.MSG_INVALID_AGGREGATOR_EXPRESSION);
		}

		// Find out aggregator type.
		for (Entry<Type, String[]> entry: aggregatorNameMap.entrySet()) {
			for (String name: entry.getValue()) {
				if (expr.toLowerCase().startsWith(name.toLowerCase() + "(")) {
					type = entry.getKey();
					expr = expr.toLowerCase().replace(name.toLowerCase() + "(", "");
					expr = expr.substring(0, expr.length() - 1);
				}
			}
		}
		
		if (type == null) {
			throw new IllegalArgumentException(ExceptionMessages.MSG_INVALID_AGGREGATOR_EXPRESSION);
		}
		
		// Get arguments.
		String[] argstemp = expr.split(",");
		arguments = new ArrayList<String>();
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
		if ( (type == Aggregator.Type.AGGREGATE_BY && arguments.size() != 2)
			|| (type == Aggregator.Type.AGGREGATE_RANGE && arguments.size() != 1)) {
			throw new IllegalArgumentException(ExceptionMessages.MSG_INVALID_AGGREGATOR_EXPRESSION);
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
}
