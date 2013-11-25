package edu.ucla.nesl.sensorsafe.db.informix;

public class ExceptionMessages {
	public static final String VALID_TIMESTAMP_FORMAT = "\"YYYY-MM-DD HH:MM:SS.[SSSSS]\"";
	public static final String MSG_INVALID_TIMESTAMP_FORMAT = "Invalid timestamp format. Expected format is " + VALID_TIMESTAMP_FORMAT;
	public static final String MSG_INVALID_CRON_EXPRESSION = "Invalid cron expression. Expects [ sec(0-59) min(0-59) hour(0-23) day of month(1-31) month(1-12) day of week(0-6,Sun-Sat) ]";
	public static final String MSG_UNSUPPORTED_TUPLE_FORMAT = "Unsupported tuple format.";
	public static final String MSG_INVALID_CALENDAR_TYPE = "Invalid calendar type. Supported types are: 1min, 15min, 30min, 1hour, 1day, 1week, 1month, 1year";
	public static final String MSG_INVALID_AGGREGATOR_EXPRESSION = "Invalid aggregator expression.";
	public static final String MSG_INVALID_NUM_AGGREGATOR_ARGUMENTS = "Invalid number of aggregator arguments was provided.";
}
