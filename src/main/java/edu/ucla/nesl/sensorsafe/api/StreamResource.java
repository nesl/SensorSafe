package edu.ucla.nesl.sensorsafe.api;

import java.awt.Font;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.naming.NamingException;
import javax.servlet.http.HttpSession;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.StreamingOutput;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.ucla.nesl.sensorsafe.auth.Roles;
import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.Channel;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.model.Stream;
import edu.ucla.nesl.sensorsafe.tools.Log;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("/streams")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/streams", description = "Operations about streams.")
@PermitAll
public class StreamResource {

	private static final int ROW_LIMIT_WITHOUT_HTTP_STREAMING = 100;
	private static final long MAX_PLOT_DATA_LIMIT = 100000;
	private static final String TEMP_ZIP_FILE_PREFIX = "/tmp/zip_file_";
	private static final int BUFFER_SIZE = 4096;

	private static final String GET_STREAM_NOTES =
			"<BR>"
					+ "<b>filter</b><BR>"
					+ "<BR>"
					+ "- You can use any valid SQL expression with variable name 'timestamp' and channel names defined for the stream.<BR>"
					+ "&emsp;  Examples:<BR>"
					+ "&emsp;&emsp;	- timestamp between '2013-03-07 09:00:00' and '2013-03-14 18:00:00'<BR>"
					+ "&emsp;&emsp;	- value > 1.5 and value < 2.0<BR>"
					+ "<BR>"
					+ "- Additional expressions:<BR>"
					+ "&emsp; - Cron time: [ sec(0-59) min(0-59) hour(0-23) day of month(1-31) month(1-12) day of week(0-6,Sun-Sat) ]<BR>"
					+ "&emsp;&emsp;	 e.g., [ * * 9-18 * * 1-5 ]<BR>"
					+ "&emsp; - SQL date time parts: SECOND, MINUTE, HOUR, DAY, MONTH, WEEKDAY(0-6 Sun-Sat), YEAR<BR>"
					+ "&emsp;&emsp;	 e.g., WEEKDAY(timestamp) between 1 and 5<BR>"
					+ "&emsp; - Macros: e.g., $(MACRO_NAME)<BR>"
					+ "&emsp; - Condition on other stream: OTHER_STREAM_NAME.CHANNEL_NAME<BR>"
					+ "&emsp;&emsp;  e.g., activity.value = 'still'<BR>"
					+ "&emsp; - GPS distance function in meters: gps_distance(latitude1, longitude1, latitude2, longitude2)<BR>"
					+ "&emsp;&emsp;  e.g., gps_distance(location.latitude, location.longitude, 34.052233, -118.308862) <= 500.0<BR>"
					+ "<BR>"
					+ "<BR>"
					+ "<b>aggregator</b><BR>"
					+ "<BR>"
					+ "Examples:<BR>"
					+ "&emsp;  AggregateBy( \"avg($channel1), avg($channel2), avg($channel3)\", \"1week\")<BR>"
					+ "&emsp;  NoisyAggregateBy( \"avg($channel1), avg($channel2), avg($channel3)\", \"1week\", 0.01)<BR>"
					+ "&emsp;  AggregateRange( \"avg($value), max($value)\" )<BR>"
					+ "&emsp;  NoisyAggregateRange( \"avg($value), max($value)\", 0.01 )<BR>"
					+ "<BR>"
					+ "- AggregateBy(expression, calendar)<BR>"
					+ "&emsp;  Down sample time series to the calendar, e.g., every 1 seconds to every 1 hour.<BR>"
					+ "- AggregateRange(expression)<BR>"
					+ "&emsp;  Calculate expression on entire range specified, e.g., average from January to June.<BR>"
					+ "- NoisyAggregateBy(expression, calendar, epsilon)<BR>"
					+ "- NoisyAggregateRange(expression, epsilon)<BR>"
					+ "&emsp;  epsilon-differentially private version of the function.<BR>"
					+ "<BR>"
					+ "+ expression: MIN, MAX, MEDIAN, SUM, AVG, FIRST, LAST, or Nth.<BR>"
					+ "&emsp;  e.g., min($value), max($value), median($value), sum($value), avg($value), first($value), last($value), Nth($value, 10)<BR>"
					+ "+ calendar: 1min, 15min, 30min, 1hour, 1day, 1week, 1month, or 1year<BR>"
					+ "+ epsilon: Prameter to e-differentially private noise generator. The smaller, the more private. Typical value smaller than 0.1.<BR>"
					+ "<BR>";

	@Context
	private SecurityContext securityContext;

	@Context
	private HttpSession session;

	@RolesAllowed({ Roles.OWNER, Roles.CONSUMER })
	@GET
	@ApiOperation(value = "Get list of streams", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Internal Server Error")
	})
	public List<Stream> doGetAllStreams(
			@ApiParam(name = "stream_owner", value = "If null, get currently authenticated user's streams.")
			@QueryParam("stream_owner") String streamOwner) throws JsonProcessingException {

		if (streamOwner == null) {
			streamOwner = securityContext.getUserPrincipal().getName();
		}

		List<Stream> streams = null;
		StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			streams = db.getStreamList(streamOwner);			
		} catch (SQLException | ClassNotFoundException | IOException | NamingException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return streams;
	}

	@RolesAllowed(Roles.OWNER)
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Create a new stream", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Internal Server Error")
	})
	public ResponseMsg doPostNewStream(@Valid Stream stream) throws JsonProcessingException {
		StreamDatabaseDriver db = null;
		String ownerName = securityContext.getUserPrincipal().getName();
		stream.setOwner(ownerName);
		try {
			db = DatabaseConnector.getStreamDatabase();
			db.createStream(stream);
		} catch (IOException | NamingException | SQLException | ClassNotFoundException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} catch (IllegalArgumentException e) {
			throw WebExceptionBuilder.buildBadRequest(e);
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return new ResponseMsg("Successfully created a new stream: " + stream.name);
	}

	@RolesAllowed(Roles.OWNER)
	@DELETE
	@ApiOperation(value = "Delete entire streams.", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Internal Server Error")
	})
	public ResponseMsg doDeleteAllStreams() throws JsonProcessingException {
		String ownerName = securityContext.getUserPrincipal().getName();
		StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			db.deleteAllStreams(ownerName);
		} catch (SQLException | ClassNotFoundException | IOException | NamingException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return new ResponseMsg("Successfully deleted all streams.");
	}

	@RolesAllowed(Roles.OWNER)
	@POST
	@Path("/{stream_name}.csv")
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	@ApiOperation(value = "Load csv file to a stream.", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Interval Server Error")
	})
	public ResponseMsg doPostStreamCsv(@PathParam("stream_name") String streamName,
			@ApiParam(name = "str_tuple", 
			value = "<pre>Usage:\n"
					+ "timestamp, 1st_channel, 2nd_channel, 3rd_channel, ..\n"
					+ "timestamp, 1st_channel, 2nd_channel, 3rd_channel, ..\n"
					+ ".\n"
					+ ".\n"
					+ "\n"
					+ "e.g.,\n"
					+ "2013-01-01 09:20:12.12345, 12.4, 1.2, 5.5\n"
					+ "2013-01-01 09:20:13.12345, 11.4, 3.2, 1.5\n"
					+ "2013-01-01 09:20:14.12345, 10.4, 4.2, 7.5\n"
					+ "</pre>")
	String data) throws JsonProcessingException {

		String ownerName = securityContext.getUserPrincipal().getName();
		StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			db.bulkLoad(ownerName, streamName, data);
		} catch (SQLException | IOException | ClassNotFoundException | NamingException | NoSuchAlgorithmException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} catch (IllegalArgumentException e) {
			throw WebExceptionBuilder.buildBadRequest(e);
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return new ResponseMsg("Successfully completed bulkloading.");
	}

	private String newUUIDString() {
		String tmp = UUID.randomUUID().toString();
		return tmp.replaceAll("-", "");
	}

	public static String unzip(String zipFile, String location) throws IOException {
		int size;
		byte[] buffer = new byte[BUFFER_SIZE];

		if ( !location.endsWith("/") ) {
			location += "/";
		}
		File f = new File(location);
		if(!f.isDirectory()) {
			f.mkdirs();
		}
		ZipInputStream zin = new ZipInputStream(new BufferedInputStream(new FileInputStream(zipFile), BUFFER_SIZE));
		String fileName = null;
		try {
			ZipEntry ze = null;

			while ((ze = zin.getNextEntry()) != null) {
				String path = location + ze.getName();
				File unzipFile = new File(path);

				if (ze.isDirectory()) {
					throw new UnsupportedOperationException("Error: The zip file contains directory.");
				} else {
					if (fileName != null) {
						throw new UnsupportedOperationException("Error: The zip file contains more than a single file.");
					}

					fileName = path;

					// check for and create parent directories if they don't exist
					File parentDir = unzipFile.getParentFile();
					if ( null != parentDir ) {
						if ( !parentDir.isDirectory() ) {
							parentDir.mkdirs();
						}
					}

					// unzip the file
					FileOutputStream out = new FileOutputStream(unzipFile, false);
					BufferedOutputStream fout = new BufferedOutputStream(out, BUFFER_SIZE);
					try {
						while ( (size = zin.read(buffer, 0, BUFFER_SIZE)) != -1 ) {
							fout.write(buffer, 0, size);
						}
						zin.closeEntry();
					}
					finally {
						fout.flush();
						fout.close();
					}
				}
			}
		}
		finally {
			zin.close();
		}

		return fileName;
	}

	private static String readEntireFile(String filename) throws IOException {
		FileReader in = new FileReader(filename);
		StringBuilder contents = new StringBuilder();
		char[] buffer = new char[BUFFER_SIZE];
		int read = 0;
		do {
			contents.append(buffer, 0, read);
			read = in.read(buffer);
		} while (read >= 0);
		in.close();
		return contents.toString();
	}

	public static void deleteFile(File file) {
		if(file.isDirectory()) {
			//directory is empty, then delete it
			if(file.list().length == 0) {
				file.delete();
			} else {
				//list all the directory contents
				String files[] = file.list();

				for (String temp : files) {
					//construct the file structure
					File fileDelete = new File(file, temp);

					//recursive delete
					deleteFile(fileDelete);
				}

				//check the directory again, if empty then delete it
				if(file.list().length == 0) {
					file.delete();
				}
			}

		} else {
			//if file, then delete it
			file.delete();
		}
	}

	@RolesAllowed(Roles.OWNER)
	@POST
	@Path("/{stream_name}.zip")
	@Consumes("application/zip")
	@ApiOperation(value = "Load zip file to a stream.", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Interval Server Error")
	})
	public ResponseMsg doPostStreamZip(@PathParam("stream_name") String streamName,
			@ApiParam(name = "str_tuple", 
			value = "<pre>Usage: attach a zip file containing a single file with the following content:\n"
					+ "timestamp, 1st_channel, 2nd_channel, 3rd_channel, ..\n"
					+ "timestamp, 1st_channel, 2nd_channel, 3rd_channel, ..\n"
					+ ".\n"
					+ ".\n"
					+ "\n"
					+ "e.g.,\n"
					+ "2013-01-01 09:20:12.12345, 12.4, 1.2, 5.5\n"
					+ "2013-01-01 09:20:13.12345, 11.4, 3.2, 1.5\n"
					+ "2013-01-01 09:20:14.12345, 10.4, 4.2, 7.5\n"
					+ "</pre>")
	byte[] data) throws JsonProcessingException {

		// Create temporary file
		DataOutputStream os = null;
		File file = null;
		String zipFileName = null;
		String unzipFolderName = null;
		try {
			unzipFolderName = TEMP_ZIP_FILE_PREFIX + newUUIDString(); 
			zipFileName = unzipFolderName + ".zip";
			file = new File(zipFileName);
			if (file.exists()) {
				file.delete();				
			}
			file.createNewFile();

			os = new DataOutputStream(new FileOutputStream(file));
			os.write(data);
		} catch (IOException e) {
			deleteTempZipFiles(zipFileName, unzipFolderName);
			throw WebExceptionBuilder.buildInternalServerError(e);
		} finally {
			try {
				if (os != null)
					os.close();
			} catch (IOException e) {
				throw WebExceptionBuilder.buildInternalServerError(e);
			}
		}

		// Unzip the file.
		String unzippedFileName = null;
		try {
			unzippedFileName = unzip(zipFileName, unzipFolderName);
		} catch (IOException | UnsupportedOperationException e) {
			deleteTempZipFiles(zipFileName, unzipFolderName);
			throw WebExceptionBuilder.buildInternalServerError(e);
		}

		// Check unzipped filename
		if (unzippedFileName == null) {
			deleteTempZipFiles(zipFileName, unzipFolderName);
			throw WebExceptionBuilder.buildBadRequest("Problem with Zip file.");
		}
		
		// Read the file into memory.
		String unzippedData = null;
		try {
			unzippedData = readEntireFile(unzippedFileName);
		} catch (IOException e) {
			deleteTempZipFiles(zipFileName, unzipFolderName);
			throw WebExceptionBuilder.buildInternalServerError(e);
		}

		if (unzippedData.length() <= 0) {
			deleteTempZipFiles(zipFileName, unzipFolderName);
			throw WebExceptionBuilder.buildBadRequest("Empty data.");
		}
		
		// Pass unzipped data to database
		String ownerName = securityContext.getUserPrincipal().getName();
		StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			db.bulkLoad(ownerName, streamName, unzippedData);
		} catch (SQLException | IOException | ClassNotFoundException | NamingException | NoSuchAlgorithmException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} catch (IllegalArgumentException e) {
			throw WebExceptionBuilder.buildBadRequest(e.getClass().getSimpleName() + ": " + e.getMessage());
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
			deleteTempZipFiles(zipFileName, unzipFolderName);
		}

		return new ResponseMsg("Successfully completed zip bulkloading.");
	}

	private void deleteTempZipFiles(String zipFileName, String unzipFolderName) {
		// Delete temporary files
		File zipFile = new File(zipFileName);
		File zipFolder = new File(unzipFolderName);
		deleteFile(zipFile);
		deleteFile(zipFolder);
	}

	@RolesAllowed(Roles.OWNER)
	@POST
	@Path("/{stream_name}")
	@Consumes(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Add a new tuple to the stream.", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Interval Server Error")
	})
	public ResponseMsg doPostStream(
			@PathParam("stream_name") String streamName, 
			@ApiParam(name = "str_tuple", 
			value = "<pre>Usage:\n"
					+ "[ timestamp, 1st_channel, 2nd_channel, 3rd_channel, .. ]\n"
					+ "\n"
					+ "  e.g., [ \"2013-01-01 09:20:12.12345\", 12.4, 1.2, 5.5 ]\n"
					+ "  e.g., [ null, 12.4, 1.2, 5.5 ]\n"
					+ "\n"
					+ "Or,\n"
					+ "{ \"timestamp\": timestamp\n"
					+ "  \"tuple\": [ 1st_channel, 2nd_channel, 3rd_channel, .. ] }\n"
					+ "\n"
					+ "  e.g., { \"timestamp\": \"2013-01-01 09:20:12.12345\"\n"
					+ "          \"tuple\": [ 12.4, 1.2, 5.5 ] }\n"
					+ "  e.g., { \"timestamp\": null\n"
					+ "          \"tuple\": [ 12.4, 1.2, 5.5 ] }\n"
					+ "\n"
					+ "If timestamp is null, current server time will be used.</pre>") 
			String strTuple) throws JsonProcessingException {

		String ownerName = securityContext.getUserPrincipal().getName();
		StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			db.addTuple(ownerName, streamName, strTuple);
		} catch (ClassNotFoundException | IOException | NamingException | SQLException e) {
			e.printStackTrace();
			throw WebExceptionBuilder.buildInternalServerError(e);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			throw WebExceptionBuilder.buildBadRequest(e);
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return new ResponseMsg("Successfully added the tuple.");
	}

	@RolesAllowed({ Roles.OWNER, Roles.CONSUMER })
	@GET
	@Path("/{stream_name}.png")
	@Produces("image/png")
	@ApiOperation(value = "Retrieve the stream.", notes = GET_STREAM_NOTES)
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Internal Server Error")
	})
	public Response doGetStreamAsPng(
			@PathParam("stream_name") 					final String streamName,			
			@ApiParam(name = "stream_owner", value = "If null, get currently authenticated user's streams.")
			@QueryParam("stream_owner")					final String streamOwnerParam,
			@ApiParam(name = "start_time", value = "Expected format: YYYY-MM-DD HH:MM:SS.[SSSSS]")
			@QueryParam("start_time") 					final String startTime, 
			@ApiParam(name = "end_time", value = "Expected format: YYYY-MM-DD HH:MM:SS.[SSSSS]")
			@QueryParam("end_time") 					final String endTime,
			@ApiParam(name = "filter", value = "Please refer to the above Implementation Notes.")
			@QueryParam("filter") 						final String filter,
			@ApiParam(name = "aggregator", value = "Please refer to the above Implementation Notes.")
			@QueryParam("aggregator")					String aggregator,
			/*@ApiParam(name = "limit", value = "Default value is 100.") 
			@DefaultValue("100") @QueryParam("limit") 	final int limit,
			@ApiParam(name = "offset", value = "Default value is 0.") 
			@QueryParam("offset") 						final int offset,*/
			@ApiParam(name = "width", value = "Chart width in pixels. Default is 500.") 
			@DefaultValue("500") @QueryParam("width") 	int width,
			@ApiParam(name = "height", value = "Chart width in pixels. Default is 300.") 
			@DefaultValue("300") @QueryParam("height") 	int height
			) throws IOException {

		StreamDatabaseDriver db = null;
		String requestingUser = securityContext.getUserPrincipal().getName();		
		String streamOwner = streamOwnerParam == null ? requestingUser : streamOwnerParam;

		try {
			db = DatabaseConnector.getStreamDatabase();

			boolean isData = db.prepareQuery(requestingUser, streamOwner, streamName, startTime, endTime, aggregator, filter, 0, 0, 0, true, null);
			Stream stream = db.getStoredStreamInfo();

			if (!isData) {
				return Response.ok("No data").build();
			}

			if (stream.num_samples > width) {
				db.close();
				db = DatabaseConnector.getStreamDatabase();
				int skipEveryNth = (int) (stream.num_samples / width);
				isData = db.prepareQuery(requestingUser, streamOwner, streamName, startTime, endTime, aggregator, filter, 0, 0, skipEveryNth, true, null);
				stream = db.getStoredStreamInfo();
				
				if (!isData) {
					return Response.ok("No data").build();
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
			String filename = ServletUtilities.saveChartAsPNG(chart, width, height, info, session);

			File imageFile = new File("/tmp/" + filename);
			byte[] imageData = FileUtils.readFileToByteArray(imageFile);
			imageFile.delete();
			
			// Send non-streamed
			//return Response.ok(imageData).build();
			
			// Send streamed
			return Response.ok(new ByteArrayInputStream(imageData)).build();
		} catch (ClassNotFoundException | IOException | NamingException | SQLException | UnsupportedOperationException e) {
			e.printStackTrace();
			throw WebExceptionBuilder.buildInternalServerError(e);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			throw WebExceptionBuilder.buildBadRequest(e);
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

	@RolesAllowed({ Roles.OWNER, Roles.CONSUMER })
	@GET
	@Path("/{stream_name}")
	@ApiOperation(value = "Retrieve the stream.", notes = GET_STREAM_NOTES)
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Internal Server Error")
	})
	public Object doGetStream(
			@PathParam("stream_name") 					final String streamName,			
			@ApiParam(name = "stream_owner", value = "If null, get currently authenticated user's streams.")
			@QueryParam("stream_owner")					final String streamOwnerParam,
			@ApiParam(name = "start_time", value = "Expected format: YYYY-MM-DD HH:MM:SS.[SSSSS]")
			@QueryParam("start_time") 					final String startTime, 
			@ApiParam(name = "end_time", value = "Expected format: YYYY-MM-DD HH:MM:SS.[SSSSS]")
			@QueryParam("end_time") 					final String endTime,
			@ApiParam(name = "filter", value = "Please refer to the above Implementation Notes.")
			@QueryParam("filter") 						final String filter,
			@ApiParam(name = "aggregator", value = "Please refer to the above Implementation Notes.")
			@QueryParam("aggregator")					final String aggregator,
			@ApiParam(name = "limit", value = "Default value is 100.") 
			@DefaultValue("100") @QueryParam("limit") 	final int limit,
			@ApiParam(name = "offset", value = "Default value is 0.") 
			@QueryParam("offset") 						final int offset,
			@ApiParam(name = "http_streaming", value = "Default value is true.") 
			@DefaultValue("false") @QueryParam("http_streaming") final boolean isHttpStreaming
			) throws JsonProcessingException {

		StreamDatabaseDriver db = null;
		final String requestingUser = securityContext.getUserPrincipal().getName();		
		final String streamOwner = streamOwnerParam == null ? requestingUser : streamOwnerParam;
		try {
			db = DatabaseConnector.getStreamDatabase();
			if (!isHttpStreaming && limit > ROW_LIMIT_WITHOUT_HTTP_STREAMING) {
				throw WebExceptionBuilder.buildBadRequest("Too mcuh data requested without HTTP streaming.");
			}

			if (!isHttpStreaming) {
				boolean isData = db.prepareQuery(requestingUser, streamOwner, streamName, startTime, endTime, aggregator, filter, limit, offset, 0, false, null);
				String strJson = getStreamJsonPrefix(db.getStoredStreamInfo());
				if (isData) {
					Object[] tuple = db.getNextTuple();
					if (tuple != null) {
						strJson += convertTupleToJsonString(tuple);
						while ((tuple = db.getNextTuple()) != null) {
							strJson += "," + convertTupleToJsonString(tuple);
						}
					}
				} 
				return strJson + "]}";
			} else {
				return new StreamingOutput() {
					@Override
					public void write(OutputStream output) throws IOException, WebApplicationException {
						StreamDatabaseDriver db = null;
						try {
							db = DatabaseConnector.getStreamDatabase();
							boolean isData = db.prepareQuery(requestingUser, streamOwner, streamName, startTime, endTime, aggregator, filter, limit, offset, 0, false, null);
							String strJson = getStreamJsonPrefix(db.getStoredStreamInfo());
							IOUtils.write(strJson, output);
							if (isData) {
								Object[] tuple = db.getNextTuple();
								if (tuple != null) {
									IOUtils.write(convertTupleToJsonString(tuple), output);
									while((tuple = db.getNextTuple()) != null) {
										IOUtils.write("," + convertTupleToJsonString(tuple), output);
									}
								}
							}
							IOUtils.write("]}", output);
						} catch (SQLException | ClassNotFoundException | NamingException | UnsupportedOperationException e) {
							e.printStackTrace();
							throw WebExceptionBuilder.buildInternalServerError(e);
						} catch (IllegalArgumentException e) {
							e.printStackTrace();
							throw WebExceptionBuilder.buildBadRequest(e);
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
				};
			}
		} catch (ClassNotFoundException | IOException | NamingException | SQLException | UnsupportedOperationException e) {
			e.printStackTrace();
			throw WebExceptionBuilder.buildInternalServerError(e);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			throw WebExceptionBuilder.buildBadRequest(e);
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

	private String convertTupleToJsonString(Object[] tuple) {
		Timestamp ts = new Timestamp(((Long)tuple[0]).longValue());
		tuple[0] = ts.toString();
		JSONArray jsonArray = new JSONArray();
		jsonArray.addAll(Arrays.asList(tuple));
		return jsonArray.toString();
	}

	private String getStreamJsonPrefix(Stream stream) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		AnnotationIntrospector introspector = new JaxbAnnotationIntrospector();
		mapper.setAnnotationIntrospector(introspector);
		JSONObject json = (JSONObject)JSONValue.parse(mapper.writeValueAsString(stream));
		String strJson = json.toString();
		return strJson.substring(0, strJson.length() - 1) + ",\"tuples\":[";
	}

	@RolesAllowed(Roles.OWNER)
	@DELETE
	@Path("/{stream_name}")
	@ApiOperation(value = "Delete a stream.", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Internal Server Error")
	})
	public ResponseMsg doDeleteStream(
			@PathParam("stream_name") 	String streamName,
			@QueryParam("start_time") 	String startTime, 
			@QueryParam("end_time") 	String endTime) throws JsonProcessingException {

		String ownerName = securityContext.getUserPrincipal().getName();
		StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			db.deleteStream(ownerName, streamName, startTime, endTime);
		} catch (ClassNotFoundException | IOException | NamingException | SQLException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} catch (IllegalArgumentException e) {
			throw WebExceptionBuilder.buildBadRequest(e);
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return new ResponseMsg("Succefully deleted stream (" + streamName + ").");
	}	
}
