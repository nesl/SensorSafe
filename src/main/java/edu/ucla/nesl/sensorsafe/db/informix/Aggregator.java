package edu.ucla.nesl.sensorsafe.db.informix;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Aggregator {
	public static enum Type { AGGREGATE_BY, AGGREGATE_RANGE };
	public static Map<Type, String[]> aggregatorNameMap = new HashMap<Type, String[]>();

	static {
		aggregatorNameMap.put(Type.AGGREGATE_BY, new String[] { "AggregateBy", "DownSample" });
		aggregatorNameMap.put(Type.AGGREGATE_RANGE, new String[] { "AggregateRange", "Calculate" });
	}

	public Type type;
	public String[] arguments;

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
		arguments = expr.split(",");
				
		for (int i = 0; i < arguments.length; i++) {
			arguments[i] = arguments[i].trim(); 
		}
		
		// Check argument validity
		if ( (type == Aggregator.Type.AGGREGATE_BY && arguments.length != 2)
			|| (type == Aggregator.Type.AGGREGATE_RANGE && arguments.length != 1)) {
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
