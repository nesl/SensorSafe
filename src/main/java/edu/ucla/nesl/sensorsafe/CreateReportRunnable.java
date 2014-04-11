package edu.ucla.nesl.sensorsafe;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import javax.mail.MessagingException;
import javax.naming.NamingException;

import org.apache.commons.io.FileUtils;
import org.jfree.chart.ChartRenderingInfo;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.NumberTickUnit;
import org.jfree.chart.entity.StandardEntityCollection;
import org.jfree.chart.plot.IntervalMarker;
import org.jfree.chart.plot.Marker;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.StandardXYItemRenderer;
import org.jfree.chart.servlet.ServletUtilities;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.Layer;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.db.UserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.Channel;
import edu.ucla.nesl.sensorsafe.model.Stream;
import edu.ucla.nesl.sensorsafe.tools.Log;
import edu.ucla.nesl.sensorsafe.tools.MailSender;

public class CreateReportRunnable implements Runnable {

	private static final String REPORTS_URL = "/reports";
	private static final String EMAIL_TITLE = "Your reports are ready!";
	
	private static final String LOCATION_SENSOR = "Location";
	private static final String ACTIVITY_SENSOR = "Activity";
	private static final String STRESS_SENSOR = "Stress";
	private static final String CONVERSATION_SENSOR = "Conversation";
	private static final String PHONE_ACCELEROMETER_SENSOR = "PhoneAccelerometer";
	private static final String RIP_SENSOR = "RIP";
	private static final String ECG_SENSOR = "ECG";
	private static final String[] REPORT_SENSORS = { LOCATION_SENSOR, ACTIVITY_SENSOR, STRESS_SENSOR, CONVERSATION_SENSOR, PHONE_ACCELEROMETER_SENSOR, RIP_SENSOR, ECG_SENSOR };
	private static final String GPS_STREAM_NAME = "PhoneGPS";

	private static final String DUMMY_STREAM_NAME = "dummy_stream"; 
	private static final String DUMMY_STREAM_CHANNEL_NAME = "value";
	private static final String DUMMY_STREAM_CHANNEL_TYPE = "int";
	private static final String OTHER_USER_NAME = "researcher";
	
	private static final int MAX_SEC_A_DAY = 3600 * 24; // seconds
	private static final int DUMMY_STREAM_INTERVAL = 5 * 60; // seconds

	private static final int CHART_WIDTH = 1200;
	private static final int CHART_HEIGHT = 400;

	public static final Object reportLock = new Object();
	public static final String REPORT_ROOT = "/opt/jetty/sensorsafe_reports";
	public static final String REPORT_LOCK_FILE = REPORT_ROOT + "/report_lock";
	
	private static final String REPORT_TEMPLATE_PATH = REPORT_ROOT + "/templates/report_template.html";
	private static final String REPORT_INDEX_TEMPLATE_PATH = REPORT_ROOT + "/templates/report_index_template.html";
	private static final String NO_DATA_TEMPLATE = REPORT_ROOT + "/templates/no_data_template.html";
	
	private static final int EARTH_METERS = 6367000;

	private static final int GPS_CLUSTER_METER = 150; // meters
	private static final int GPS_CLUSTER_TIME = 5 * 60; // seconds


	private DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
	private DateTimeFormatter fileFmt = DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm-ss");
	private DateTimeFormatter simpleFmt = DateTimeFormat.forPattern("yyyy-MM-dd");
	private DateTimeFormatter titleFmt = DateTimeFormat.forPattern("MM/dd hh:mm aa");

	private final DateTime startDate;
	private final DateTime endDate;
	private final String owner;

	private double minLat;
	private double minLon;
	private double maxLat;
	private double maxLon;

	private String mGPSSamplesAllShared;
	private String mGPSSamplesSharingFlagged;
	
	private String basePath;
	
	class Coord {
		public int timestampInSecs;
		public double lat;
		public double lon;

		public Coord(int timestampInSecs, double lat, double lon) {
			this.timestampInSecs = timestampInSecs; 
			this.lat = lat;
			this.lon = lon;
		}
	}

	class Range {
		public double startTimeInMillis;
		public double endTimeInMillis;

		public Range(double startTimeInMillis, double endTimeInMillis) {
			this.startTimeInMillis = startTimeInMillis;
			this.endTimeInMillis = endTimeInMillis;
		}
	}

	public CreateReportRunnable(DateTime startDate, DateTime endDate, String owner, String basePath) {
		this.startDate = startDate;
		this.endDate = endDate;
		this.owner = owner;
		this.basePath = basePath;
		File reportRoot = new File(REPORT_ROOT);
		if (!reportRoot.exists()) {
			reportRoot.mkdirs();
		}
	}

	@Override
	public void run() {
		try {
			if (!checkLock()) {
				return;
			}

			Log.info("Report thread running..");

			// Fetch GPS data
			Log.info("Fetching GPS data..");
			
			boolean isGPS = generateGPSInitScript(); 
			if (!isGPS) {
				Log.error("Generating GPS samples failed..");
			} else {
				Log.info("Fetching GPS data done");
			}

			// Read report template
			String reportTemplate = FileUtils.readFileToString(new File(REPORT_TEMPLATE_PATH));

			if (reportTemplate == null) {
				Log.error("Error reading report template.");
				return;
			}

			for (String curSensor : REPORT_SENSORS) {
				generateReportForSensor(curSensor, reportTemplate);
			}

			String reportIdxFileName = generateReportIndexPage();
			
			if (reportIdxFileName == null) {
				Log.error("Error generating report index.");
				return;
			}
			
			sendEmailToOwner(owner, reportIdxFileName);

			Log.info("Report thread done!.");

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			releaseLock();
		}
	}

	private String generateReportIndexPage() {

		try {
			String reportIndexFileName = REPORT_ROOT + "/" + owner + "_" + fileFmt.print(startDate) + ".html";
			String temp = FileUtils.readFileToString(new File(REPORT_INDEX_TEMPLATE_PATH));
			String titleString = owner + "'" + "s reports (" + titleFmt.print(startDate) + " ~ " + titleFmt.print(endDate) + ")"; 
			
			temp = temp.replace("<!--%{title}-->", titleString);
			
			StringBuilder sb = new StringBuilder();
			for (String sensor : REPORT_SENSORS) {
				String link = owner + "_" + fileFmt.print(startDate) + "_" + sensor;
				sb.append("<li><a href=\"./");
				sb.append(link);
				sb.append("\">");
				if (sensor.equals(RIP_SENSOR)) {
					sb.append("Breathing");
				} else {
					sb.append(sensor);
				}
				sb.append("</a></li>\n");
			}
			
			temp = temp.replace("<!--%{report_list}-->", sb.toString());
			
			File reportIndexFile = new File(reportIndexFileName);			
			FileUtils.writeStringToFile(reportIndexFile, temp);

			return reportIndexFile.getName();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private void sendEmailToOwner(String username, String reportIdxFileName) {
		String link = basePath + REPORTS_URL + "/" + reportIdxFileName;
		link = "<a href=\"" + link + "\">" + link + "</a>";
		
		String title = EMAIL_TITLE + " (" + titleFmt.print(startDate) + " ~ " + titleFmt.print(endDate) + ")";
		String body = "Your data reports are ready for your review at the following address:<br/><br/>" + link;
		body += "<br/><br/>Please access the above link and provide us your feedback!<br/><br/>";
		body += "Regards,<br/>SensorPrivacy Research Team";
		
		// Get email address
		UserDatabaseDriver db = null;
		String email = null;
		try {
			db = DatabaseConnector.getUserDatabase();
			email = db.getUserEmail(username);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NamingException e) {
			e.printStackTrace();
		} finally {
			if (db != null) { 
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		if (email == null) {
			Log.error("No email address for " + username);
			return;
		}
		
		try {
			MailSender.send(email, title, body);
		} catch (MessagingException e) {
			e.printStackTrace();
			Log.error("Error while sending email..");
			return;			
		}
		
		Log.info("Done sending email!");
	}

	private void generateReportForSensor(String curSensor, String reportTemplate) {
		// Prepare folders.
		String folderName = owner + "_" + fileFmt.print(startDate) + "_" + curSensor;
		String destFolderName = REPORT_ROOT + "/" + folderName;
		File destFolder = new File(destFolderName);
		if (!destFolder.exists()) {
			destFolder.mkdirs();
		}

		// Handle template for GPS data
		reportTemplate = reportTemplate.replace("/*%{title}*/", folderName);
		reportTemplate = processSensorName(reportTemplate, curSensor);
		if (curSensor.equals(RIP_SENSOR)) {
			reportTemplate = reportTemplate.replace("<!--%{sensor_name}-->", "Breathing");
		} else {
			reportTemplate = reportTemplate.replace("<!--%{sensor_name}-->", curSensor);
		}
		reportTemplate = reportTemplate.replace("/*%{min_latitude}*/", "minLat = " + minLat + ";");
		reportTemplate = reportTemplate.replace("/*%{max_latitude}*/", "maxLat = " + maxLat + ";");
		reportTemplate = reportTemplate.replace("/*%{min_longitude}*/", "minLon = " + minLon + ";");
		reportTemplate = reportTemplate.replace("/*%{max_longitude}*/", "maxLon = " + maxLon + ";");
		reportTemplate = reportTemplate.replace("/*%{dest_folder}*/", folderName);
		reportTemplate = reportTemplate.replace("/*%{start_epoch}*/", "var startEpoch= " + startDate.getMillis() + ";");
		reportTemplate = reportTemplate.replace("/*%{end_epoch}*/", "var endEpoch = " + endDate.getMillis() + ";");
		
		if (!curSensor.equals(LOCATION_SENSOR)) {			
			if (mGPSSamplesAllShared != null) {
				reportTemplate = reportTemplate.replace("/*%{gps_values}*/", mGPSSamplesAllShared);
			}
			boolean isLine = true;
			boolean isShape = false;
			
			if (curSensor.equals(ACTIVITY_SENSOR)) {
				isLine = false;
				isShape = true;
				reportTemplate = reportTemplate.replace("<!--%{chart_note}-->", "0 = Still, 1 = Walk, 2 = Run, 3 = Bike, 4 = Drive<br/>");
			} else if (curSensor.equals(STRESS_SENSOR)) {
				isLine = false;
				isShape = true;
				reportTemplate = reportTemplate.replace("<!--%{chart_note}-->", "0 = No stress, 1 = Stress<br/>");
			} else if (curSensor.equals(CONVERSATION_SENSOR)) {
				isLine = false;
				isShape = true;
				reportTemplate = reportTemplate.replace("<!--%{chart_note}-->", "0 = Quiet, 1 = Speaking, 2 = Smoking<br/>");
			}
			
			File pngFile = createPNG(curSensor, CHART_WIDTH, CHART_HEIGHT, destFolderName, isLine, isShape);
			
			if (pngFile == null) {
				String reportFileName = destFolderName + "/index.html";
				File reportFile = new File(reportFileName);

				try {
					String noDataTemplate = FileUtils.readFileToString(new File(NO_DATA_TEMPLATE));
					noDataTemplate = noDataTemplate.replace("<!--%{title}-->", folderName);
					noDataTemplate = processSensorName(noDataTemplate, curSensor);
					FileUtils.writeStringToFile(reportFile, noDataTemplate);
				} catch (IOException e) {
					e.printStackTrace();
				}
				return;
			}
			
			reportTemplate = reportTemplate.replace("<!--%{chart_image}-->"
					, "<img src=\"" + pngFile.getName() + "\"/>");
			
			if (mGPSSamplesAllShared != null) {
				reportTemplate = reportTemplate.replace("/*%{init_script}*/"
						, "$(\".location_content\").hide();\n"
						  + "$(\".non_location_content\").show();\n");
			} else {
				reportTemplate = reportTemplate.replace("/*%{init_script}*/"
						, "$(\".location_content\").hide();\n"
						  + "$(\".non_location_content\").show();\n"
						  + "$(\".no_location_data\").hide();\n");
			}
		} else {
			if (mGPSSamplesSharingFlagged == null) {
				String reportFileName = destFolderName + "/index.html";
				File reportFile = new File(reportFileName);

				try {
					String noDataTemplate = FileUtils.readFileToString(new File(NO_DATA_TEMPLATE));
					noDataTemplate = noDataTemplate.replace("<!--%{title}-->", folderName);
					noDataTemplate = processSensorName(noDataTemplate, curSensor);
					FileUtils.writeStringToFile(reportFile, noDataTemplate);
				} catch (IOException e) {
					e.printStackTrace();
				}
				return;
			}
			reportTemplate = reportTemplate.replace("/*%{gps_values}*/", mGPSSamplesSharingFlagged);
			reportTemplate = reportTemplate.replace("/*%{init_script}*/"
					, "$(\".location_content\").show();\n"
					  + "$(\".non_location_content\").hide();\n");
		}

		// Write the report.
		String reportFileName = destFolderName + "/index.html";
		File reportFile = new File(reportFileName);

		try {
			FileUtils.writeStringToFile(reportFile, reportTemplate);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String processSensorName(String reportTemplate, String curSensor) {
		if (curSensor.equals(RIP_SENSOR)) {
			reportTemplate = reportTemplate.replace("<!--%{sensor_name}-->", "Breathing");
		} else {
			reportTemplate = reportTemplate.replace("<!--%{sensor_name}-->", curSensor);
		}
		return reportTemplate;
	}

	private boolean generateGPSInitScript() {

		StreamDatabaseDriver db = null;
		String requestingUser = owner;	
		String streamOwner = owner;
		String startTime = fmt.print(startDate);
		String endTime = fmt.print(endDate);

		long startTimestamp = startDate.getMillis();

		try {
			db = DatabaseConnector.getStreamDatabase();

			boolean isData = db.prepareQuery(requestingUser, streamOwner, GPS_STREAM_NAME, startTime, endTime, null, null, 0, 0, 0, true, null);

			if (!isData) {
				Log.error("No GPS data..");
				return false;
			}

			Stream stream = db.getStoredStreamInfo();
			long numSamples = stream.num_samples;

			if (numSamples <= 0) {
				Log.error("numSamples <= 0 on " + GPS_STREAM_NAME);
				return false;
			}
			
			Object[] tuple = new Object[db.getStoredStreamInfo().channels.size() + 1];
			int timestamp;
			Coord coords[] = new Coord[(int)numSamples];
			int idx = 0;
			while (db.getNextTuple(tuple)) {
				timestamp = (int)(((Long)tuple[0] - startTimestamp) / 1000);
				coords[idx] = new Coord(timestamp, (Double)tuple[1], (Double)tuple[2]);
				idx++;
			}

			List<Coord> clusteredCoords = gpsClustering(coords);

			// Generate all shared gps poitns.
			StringBuilder sb = new StringBuilder();
			for (Coord coord : clusteredCoords) {
				sb.append("gps[");
				sb.append(coord.timestampInSecs);
				sb.append("] = new google.maps.Marker({ position: new google.maps.LatLng(");
				sb.append(coord.lat);
				sb.append(", ");
				sb.append(coord.lon);
				sb.append("), icon: sharedCircle, draggable: false, map: null });\n");
			}
			
			mGPSSamplesAllShared = sb.toString();

			// Generate based on rule processing.
			List<Range> unsharedRanges = getUnsharedRanges(streamOwner, GPS_STREAM_NAME);
			sb = new StringBuilder();
			for (Coord coord : clusteredCoords) {
				sb.append("gps[");
				sb.append(coord.timestampInSecs);
				sb.append("] = new google.maps.Marker({ position: new google.maps.LatLng(");
				sb.append(coord.lat);
				sb.append(", ");
				sb.append(coord.lon);
				sb.append("), icon: ");
				if (isCoordUnshared(coord, unsharedRanges)) {
					sb.append("nonSharedCircle");
				} else {
					sb.append("sharedCircle");
				}
				sb.append(", draggable: false, map: null });\n");
			}
			
			mGPSSamplesSharingFlagged = sb.toString();
			
			return true;
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NamingException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return false;
	}

	private boolean isCoordUnshared(Coord coord, List<Range> unsharedRanges) {
		for (Range range : unsharedRanges) {
			long coordTs = coord.timestampInSecs * 1000 + startDate.getMillis();
			//Log.info("range: " + (long)range.startTimeInMillis + "~" + (long)range.endTimeInMillis + ", time: " + coordTs);
			if (coordTs >= range.startTimeInMillis && coordTs <= range.endTimeInMillis) {
				return true;
			}
		}
		return false;
	}

	private List<Coord> gpsClustering(Coord[] coords) {

		List<Coord> resultCoords = new LinkedList<Coord>();

		int curTs = coords[0].timestampInSecs + GPS_CLUSTER_TIME;
		resultCoords.add(new Coord(coords[0].timestampInSecs, coords[0].lat, coords[0].lon));
		Coord prevCoord = coords[0];
		minLat = maxLat = coords[0].lat;
		minLon = maxLon = coords[0].lon;
		int idx = 1;
		while (idx < coords.length) { 
			if (curTs < coords[idx].timestampInSecs ) {
				resultCoords.add(new Coord(curTs, prevCoord.lat, prevCoord.lon));
				curTs += GPS_CLUSTER_TIME;
			} else {
				Coord lastClusteredCoord = resultCoords.get(resultCoords.size()-1);
				if (getDistance(lastClusteredCoord.lon, coords[idx].lon, lastClusteredCoord.lat, coords[idx].lat) >= GPS_CLUSTER_METER) {
					resultCoords.add(new Coord(coords[idx].timestampInSecs, coords[idx].lat, coords[idx].lon));
				}

				// update min max lat lon
				if (coords[idx].lat > maxLat) 
					maxLat = coords[idx].lat;
				if (coords[idx].lon > maxLon) 
					maxLon = coords[idx].lon;
				if (coords[idx].lat < minLat)
					minLat = coords[idx].lat;
				if (coords[idx].lon < minLon)
					minLon = coords[idx].lon;

				prevCoord = coords[idx];
				idx++;
			}
		}

		return resultCoords;
	}

	private boolean checkLock() {
		synchronized (reportLock) {
			File lockFile = new File(REPORT_LOCK_FILE);
			if (!lockFile.exists()) {
				try {
					lockFile.createNewFile();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return true;
			} else {
				return false;
			}
		}
	}

	private void releaseLock() {
		synchronized (reportLock) {
			File lockFile = new File(REPORT_LOCK_FILE);
			if (lockFile.exists()) {
				lockFile.delete();
			}
		}
	}

	private File createPNG(String streamName, int width, int height, String outputPath, boolean isLine, boolean isShape) {

		StreamDatabaseDriver db = null;
		String requestingUser = owner;	
		String streamOwner = owner;

		String startTime = fmt.print(startDate);
		String endTime = fmt.print(endDate);

		try {
			db = DatabaseConnector.getStreamDatabase();

			boolean isData = db.prepareQuery(requestingUser, streamOwner, streamName, startTime, endTime, null, null, 0, 0, 0, true, null);
			Stream stream = db.getStoredStreamInfo();

			if (!isData) {
				Log.error("isData null");
				return null;
			}

			if (stream.num_samples > width) {
				db.close();
				db = DatabaseConnector.getStreamDatabase();
				int skipEveryNth = (int) (stream.num_samples / width);
				isData = db.prepareQuery(requestingUser, streamOwner, streamName, startTime, endTime, null, null, 0, 0, skipEveryNth, false, null);
				stream = db.getStoredStreamInfo();

				if (!isData) {
					Log.error("isData null");
					return null;
				}
			}

			// Prepare data
			XYSeries[] series = null;
			long minTsInterval = Long.MAX_VALUE;  // to determine whether to use marker on the plot.
			long prevTimestamp = -1;
			Object[] tuple = new Object[db.getStoredStreamInfo().channels.size() + 1];
			while (db.getNextTuple(tuple)) {
				// Init XYSeries array
				if (series == null) {
					series = new XYSeries[tuple.length - 1];
					for (int i = 0; i < series.length; i++) {
						series[i] = new XYSeries(stream.channels.get(i).name); 
					}
				}

				long timestamp = ((Long)tuple[0]).longValue();
				for (int i = 1; i < tuple.length; i++) {
					try {
						series[i-1].add(timestamp, (Number)tuple[i]);
					} catch (ClassCastException e) {
						continue;
					}
				}

				long diff = timestamp - prevTimestamp;
				if (diff > 0 && diff < minTsInterval) {
					minTsInterval = diff;
				}

				prevTimestamp = timestamp;
			}

			db.close();
			db = null;

			if (series == null) {
				throw new UnsupportedOperationException("No data for " + streamName);
			}
			
			XYSeriesCollection xyDataset = new XYSeriesCollection();
			for (XYSeries s : series) {
				xyDataset.addSeries(s);
			}

			// Generate title string
			long start = (long)series[0].getMinX();
			long end = (long)series[0].getMaxX();
			Timestamp startTimestamp = new Timestamp(start);
			Timestamp endTimestamp = new Timestamp(end);
			String title = stream.owner + ": " 
					+ stream.name 
					+ "\n" + startTimestamp.toString() + " ~ " + endTimestamp.toString();

			//  Create the chart object
			DateAxis xAxis = new DateAxis("Time");
			xAxis.setDateFormatOverride(new SimpleDateFormat("hh:mm aa"));			
			//NumberAxis xAxis = new NumberAxis("");
			long margin = (endDate.getMillis() - startDate.getMillis()) / 24; 
			xAxis.setRange(new Date(startDate.getMillis() - margin), new Date(endDate.getMillis() + margin));
			
			NumberAxis yAxis = new NumberAxis("Value");
			yAxis.setAutoRangeIncludesZero(false);  // override default
			
			if (streamName.equals(ACTIVITY_SENSOR)) {
				yAxis.setTickUnit(new NumberTickUnit(1.0));
				yAxis.setRange(0.0, 4.0);
			} else if (streamName.equals(STRESS_SENSOR)) {
				yAxis.setTickUnit(new NumberTickUnit(1.0));
				yAxis.setRange(0.0, 1.0);
			} else if (streamName.equals(CONVERSATION_SENSOR)) {
				yAxis.setTickUnit(new NumberTickUnit(1.0));
				yAxis.setRange(0.0, 2.0);
			}

			StandardXYItemRenderer renderer;
			//long dataCount = (end - start) / minTsInterval;
			//if (dataCount <= width) { 
			//	renderer = new StandardXYItemRenderer(StandardXYItemRenderer.LINES + StandardXYItemRenderer.SHAPES);
			//} else {
			//	renderer = new StandardXYItemRenderer(StandardXYItemRenderer.LINES);
			//}
			if (isLine && isShape) {
				renderer = new StandardXYItemRenderer(StandardXYItemRenderer.LINES + StandardXYItemRenderer.SHAPES);
			} else if (isLine && !isShape) {
				renderer = new StandardXYItemRenderer(StandardXYItemRenderer.LINES);
			} else if (!isLine && isShape) {
				renderer = new StandardXYItemRenderer(StandardXYItemRenderer.SHAPES);
			} else {
				renderer = new StandardXYItemRenderer(StandardXYItemRenderer.LINES);
			}
			//renderer.setShapesFilled(true);

			XYPlot plot = new XYPlot(xyDataset, xAxis, yAxis, renderer);
			JFreeChart chart = new JFreeChart(title, new Font(Font.SANS_SERIF, Font.BOLD, 12), plot, true);
			//JFreeChart chart = new JFreeChart(title, plot);
			chart.setBackgroundPaint(java.awt.Color.WHITE);
			chart.removeLegend();
			
			// Marker
			final Color c = new Color(255, 60, 24, 63);
			List<Range> markerRanges = getUnsharedRanges(streamOwner, streamName);

			for (Range range : markerRanges) {
				Marker marker = new IntervalMarker(range.startTimeInMillis, range.endTimeInMillis, c, new BasicStroke(2.0f), null, null, 1.0f);
				plot.addDomainMarker(marker, Layer.BACKGROUND);
			}

			ChartRenderingInfo info = new ChartRenderingInfo(new StandardEntityCollection());
			String filename = ServletUtilities.saveChartAsPNG(chart, width, height, info, null);

			File imageFile = new File("/tmp/" +  filename);
			File toFile = new File(outputPath + "/" + streamName + "_" + fileFmt.print(startDate) + ".png");
			imageFile.renameTo(toFile);

			return toFile;

		} catch (ClassNotFoundException | IOException | NamingException | SQLException | UnsupportedOperationException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	private List<Range> getUnsharedRanges(String streamOwner, String streamName) throws ClassNotFoundException, SQLException, IOException, NamingException, NoSuchAlgorithmException {
		/*double start = 1392883208801.0;
		double end = 1392936762729.0;
		double d = (end - start) / 24; 
		resultRanges.add(new Range(start, start + d));
		resultRanges.add(new Range(start + 2*d, start + 3*d));
		resultRanges.add(new Range(start + 4*d, start + 5*d));
		resultRanges.add(new Range(start + 6*d, start + 7*d));
		resultRanges.add(new Range(start + 8*d, start + 9*d));*/

		prepareDummyStream(streamOwner, streamName);

		int[] shareflags = queryUsingDummyStreamAsOtherUser(streamOwner, streamName);
		
		// dump share flags
		StringBuilder sb = new StringBuilder();
		sb.append("flags = [");
		for (int f : shareflags) {
			sb.append(f);
			sb.append(",");
		}
		sb.deleteCharAt(sb.length()-1);
		sb.append("]");
		Log.info(sb.toString());
		
		// Generate unshared ranges
		List<Range> resultRanges = new ArrayList<Range>();
		int prevflag = shareflags[0];
		double curStartTime = -1;
		if (shareflags[0] == 0) {
			curStartTime = startDate.getMillis();
		}
		for (int i = 1; i < shareflags.length; i++) {
			int curflag = shareflags[i];
			if (prevflag == curflag ) {
				continue;
			} else if (prevflag == 0 && curflag == 1) {
				if (curStartTime < 0) {
					Log.error("invalid curStartTime at " + i);
				}
				double endTime = startDate.getMillis() + i * DUMMY_STREAM_INTERVAL * 1000;
				resultRanges.add(new Range(curStartTime, endTime));
				curStartTime = -1;
			} else if (prevflag == 1 && curflag == 0) {
				curStartTime = startDate.getMillis() + i * DUMMY_STREAM_INTERVAL * 1000;
			}
			
			prevflag = shareflags[i];
		}
		if (curStartTime > 0) {
			double endTime = startDate.getMillis() + shareflags.length * DUMMY_STREAM_INTERVAL * 1000;
			resultRanges.add(new Range(curStartTime, endTime));
		}

		return resultRanges;
	}

	private int[] queryUsingDummyStreamAsOtherUser(String streamOwner, String streamName) throws ClassNotFoundException, SQLException, IOException, NamingException {
		StreamDatabaseDriver db = null;
		String startTime = fmt.print(startDate);
		String endTime = fmt.print(endDate.minusMillis(1));
		Log.info("startTime: " + startTime + ", endTime: " + endTime);
		long durationInSecs = (endDate.getMillis() / 1000) - (startDate.getMillis() / 1000);
		int size = (int)(durationInSecs / DUMMY_STREAM_INTERVAL);
		if (size <= 0) {
			size = 1;
		}
		int[] shareflags = new int[size];
		Log.info("size: " + (int)(durationInSecs / DUMMY_STREAM_INTERVAL));
		try {
			db = DatabaseConnector.getStreamDatabase();
			boolean isData = db.prepareQuery(OTHER_USER_NAME, streamOwner, DUMMY_STREAM_NAME, startTime, endTime, null, null, 0, 0, 0, false, streamName);
			if (!isData) {
				Log.info("No data shared with " + OTHER_USER_NAME + " for " + streamName);
				return shareflags;
			}
			Object[] tuple = new Object[db.getStoredStreamInfo().channels.size() + 1];
			long todayMillis = startDate.getMillis();
			while (db.getNextTuple(tuple)) {
				long timestamp = (Long)tuple[0];
				//Log.info("timestamp: " + timestamp);
				int idx = (int)((timestamp - todayMillis) / 1000 / DUMMY_STREAM_INTERVAL);
				if (idx >= size) {
					Log.error("idx >= size, timestamp = " + fmt.print(new DateTime(timestamp)) + ", idx = " + idx + ", size = " + size);
				} else {
					shareflags[idx] = 1;
				}
			}
			return shareflags;
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void prepareDummyStream(String streamOwner, String streamName) throws ClassNotFoundException, SQLException, IOException, NamingException, NoSuchAlgorithmException {

		// Check if dummy stream exists
		List<Stream> streams = null;
		StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			streams = db.getStreamList(streamOwner);			
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		boolean isFound = false;
		for (Stream stream : streams) {
			if (stream.name.equals(DUMMY_STREAM_NAME)) {
				isFound = true;
			}
		}

		if (!isFound) {
			createDummyStream(streamOwner);
		}

		// Check if dummy stream has desired amount of data on today.
		DateTime day = simpleFmt.parseDateTime(startDate.toString(simpleFmt));
		int targetNumSamples = MAX_SEC_A_DAY / DUMMY_STREAM_INTERVAL;
		
		while (day.isBefore(endDate)) {
			Log.info("current day: " + fmt.print(day));

			String startTime = fmt.print(day);
			String endTime = fmt.print(day.plusDays(1).minusMillis(1));
			try {
				db = DatabaseConnector.getStreamDatabase();

				boolean isData = db.prepareQuery(streamOwner, streamOwner, DUMMY_STREAM_NAME, startTime, endTime, null, null, 0, 0, 0, true, null);
				Stream stream = db.getStoredStreamInfo();

				if (!isData) {
					Log.error("isData false");
					throw new UnsupportedOperationException("idData false.");
				}

				if (stream.num_samples != targetNumSamples) {
					Log.info("dummy stream is no good. Bulkloading dummy stream..");
					bulkloadDummyStreamData(streamOwner, day);
				} else {
					Log.info("dummy stream is good.");
				}
			} finally {
				if (db != null) {
					try {
						db.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}

			day = day.plusDays(1);
		}
		
	}

	private void bulkloadDummyStreamData(String streamOwner, DateTime day) throws ClassNotFoundException, SQLException, IOException, NamingException, NoSuchAlgorithmException {
		DateTime curTime = new DateTime(day);
		DateTime endTime = new DateTime(day.plusDays(1).minusMillis(1));

		// delete for the day
		StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			db.deleteStream(streamOwner, DUMMY_STREAM_NAME, fmt.print(curTime), fmt.print(endTime));
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		// generate a bulkload data
		StringBuilder sb = new StringBuilder();
		while (curTime.isBefore(endTime)) {
			sb.append(fmt.print(curTime));
			sb.append(",1\n");
			curTime = curTime.plusSeconds(DUMMY_STREAM_INTERVAL);
		}

		// perform bulkload
		String bulkloadData = sb.toString();
		try {
			db = DatabaseConnector.getStreamDatabase();
			db.bulkLoad(streamOwner, DUMMY_STREAM_NAME, bulkloadData);
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void createDummyStream(String streamOwner) throws ClassNotFoundException, SQLException, IOException, NamingException {
		StreamDatabaseDriver db = null;
		Stream stream = new Stream();
		stream.setOwner(streamOwner);
		stream.name = DUMMY_STREAM_NAME;
		stream.tags = DUMMY_STREAM_NAME;
		stream.channels = new ArrayList<Channel>();
		stream.channels.add(new Channel(DUMMY_STREAM_CHANNEL_NAME, DUMMY_STREAM_CHANNEL_TYPE));
		try {
			db = DatabaseConnector.getStreamDatabase();
			db.createStream(stream);
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	// Great circle distance in meters
	public static double getDistance(double x1, double x2, double y1, double y2) {
		x1 = x1 * (Math.PI / 180);
		x2 = x2 * (Math.PI / 180);
		y1 = y1 * (Math.PI / 180);
		y2 = y2 * (Math.PI / 180);
		double dlong = x1 - x2;
		double dlat = y1 - y2;
		double a = Math.pow(Math.sin(dlat / 2), 2) + Math.cos(y1) *
				Math.cos(y2) * Math.pow(Math.sin(dlong / 2), 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		return EARTH_METERS * c;
	}
}
