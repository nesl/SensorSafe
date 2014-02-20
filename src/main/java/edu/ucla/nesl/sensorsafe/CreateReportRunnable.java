package edu.ucla.nesl.sensorsafe;

import java.awt.Font;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;

import javax.naming.NamingException;

import org.jfree.chart.ChartRenderingInfo;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.entity.StandardEntityCollection;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.StandardXYItemRenderer;
import org.jfree.chart.servlet.ServletUtilities;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.Stream;
import edu.ucla.nesl.sensorsafe.tools.Log;

public class CreateReportRunnable implements Runnable {

	public static final Object reportLock = new Object();
	private static final String REPORT_ROOT = "/opt/jetty/sensorsafe_reports";
	public static final String REPORT_LOCK_FILE = REPORT_ROOT + "/report_lock";

	private DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
	private DateTimeFormatter simpleFmt = DateTimeFormat.forPattern("yyyy-MM-dd");

	private final DateTime dateTime;
	private final String owner;

	public CreateReportRunnable(DateTime date, String owner) {
		this.dateTime = date;
		this.owner = owner;
		File reportRoot = new File(REPORT_ROOT);
		if (!reportRoot.exists()) {
			reportRoot.mkdirs();
		}
	}

	@Override
	public void run() {
		if (!checkLock()) {
			return;
		}

		Log.info("Report thread running..");

		File curFolder = new File(REPORT_ROOT + "/" + owner + "_" + simpleFmt.print(dateTime));
		if (!curFolder.exists()) {
			curFolder.mkdirs();
		}

		createPNG("PhoneAccelerometer", 1024, 500, curFolder.getAbsolutePath());
		createPNG("ECG", 1024, 500, curFolder.getAbsolutePath());
		createPNG("RIP", 1024, 500, curFolder.getAbsolutePath());

		generateHTML(curFolder.getAbsolutePath());

		Log.info("Report thread done!.");

		releaseLock();
	}

	private void generateHTML(String absolutePath) {
		File html = new File(absolutePath + "/index.html");

		FileWriter fw;
		try {
			fw = new FileWriter(html.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);

			StringBuilder content = new StringBuilder();
			content.append("<img src=\"PhoneAccelerometer_2014-02-19.png\"><br/>");
			content.append("<img src=\"ECG_2014-02-19.png\"><br/>");
			content.append("<img src=\"RIP_2014-02-19.png\"><br/>");

			bw.write(content.toString());

			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
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

	private File createPNG(String streamName, int width, int height, String outputPath) {

		StreamDatabaseDriver db = null;
		String requestingUser = owner;	
		String streamOwner = owner;

		String startTime = fmt.print(dateTime);
		String endTime = fmt.print(dateTime.plusDays(1));

		try {
			db = DatabaseConnector.getStreamDatabase();

			boolean isData = db.prepareQuery(requestingUser, streamOwner, streamName, startTime, endTime, null, null, 0, 0, 0, true);
			Stream stream = db.getStoredStreamInfo();

			if (!isData) {
				Log.error("isData null");
				return null;
			}

			if (stream.num_samples > width) {
				db.close();
				db = DatabaseConnector.getStreamDatabase();
				int skipEveryNth = (int) (stream.num_samples / width);
				isData = db.prepareQuery(requestingUser, streamOwner, streamName, startTime, endTime, null, null, 0, 0, skipEveryNth, true);
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
			ValueAxis xAxis = new DateAxis("Time");
			//NumberAxis xAxis = new NumberAxis("");
			NumberAxis yAxis = new NumberAxis("Value");
			yAxis.setAutoRangeIncludesZero(false);  // override default

			StandardXYItemRenderer renderer;
			long dataCount = (end - start) / minTsInterval;
			if (dataCount <= width) { 
				renderer = new StandardXYItemRenderer(StandardXYItemRenderer.LINES + StandardXYItemRenderer.SHAPES);
			} else {
				renderer = new StandardXYItemRenderer(StandardXYItemRenderer.LINES);
			}
			//renderer.setShapesFilled(true);

			XYPlot plot = new XYPlot(xyDataset, xAxis, yAxis, renderer);
			JFreeChart chart = new JFreeChart(title, new Font(Font.SANS_SERIF, Font.BOLD, 12), plot, true);
			//JFreeChart chart = new JFreeChart(title, plot);
			chart.setBackgroundPaint(java.awt.Color.WHITE);

			ChartRenderingInfo info = new ChartRenderingInfo(new StandardEntityCollection());
			String filename = ServletUtilities.saveChartAsPNG(chart, width, height, info, null);

			File imageFile = new File("/tmp/" +  filename);
			File toFile = new File(outputPath + "/" + streamName + "_" + simpleFmt.print(dateTime) + ".png");
			imageFile.renameTo(toFile);

			return toFile;

		} catch (ClassNotFoundException | IOException | NamingException | SQLException | UnsupportedOperationException e) {
			e.printStackTrace();
			return null;
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			return null;
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
}
