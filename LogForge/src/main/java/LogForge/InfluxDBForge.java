package logforge;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.InfluxDB.ResponseFormat;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.BoundParameterQuery.QueryBuilder;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Series;
import org.influxdb.impl.InfluxDBImpl;

public class InfluxDBForge {
	public void insertCountry(String measurement,String field , String token_md5 , String count , String latitude, String longitude) throws Exception {
		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086");
		String dbName = "logforge";
		influxDB.query(new Query("CREATE DATABASE " + dbName));
		influxDB.setDatabase(dbName);
		influxDB.write(Point.measurement(measurement)
  					.tag("tag_country", field)
  					.tag("tag_token_md5", token_md5)
  					.tag("tag_latitude", latitude)
  					.tag("tag_longitude", longitude)
  					.addField("country", field)
  					.addField("tag_token_md5", token_md5)
  					.addField("count", Integer.valueOf(count))
  					.addField("latitude", Float.valueOf(latitude))
  					.addField("longitude", Float.valueOf(longitude))
  					.build());
		influxDB.close();
	}
	public void insertUser(String measurement , String field , String index , String count) throws Exception {
		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086");
		String dbName = "logforge";
		influxDB.query(new Query("CREATE DATABASE " + dbName));
		influxDB.setDatabase(dbName);
		influxDB.write(Point.measurement(measurement)
  					.tag("tag_user", field)
  					.tag("tag_index", index)
  					.addField("user", field)
  					.addField("index", index)
  					.addField("count", Integer.valueOf(count))
  					.build());
		influxDB.close();
	}
	public void insertUserStatus(String measurement , String field , String count) throws Exception {
		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086");
		String dbName = "logforge";
		influxDB.query(new Query("CREATE DATABASE " + dbName));
		influxDB.setDatabase(dbName);
		influxDB.write(Point.measurement(measurement)
  					.tag("tag_status", field)
  					.addField("status", field)
  					.addField("count", Integer.valueOf(count))
  					.build());
		influxDB.close();
	}
	public void insertUserGuid(String measurement , String field , String token_md5 , String user_ip , String count) throws Exception {
		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086");
		String dbName = "logforge";
		influxDB.query(new Query("CREATE DATABASE " + dbName));
		influxDB.setDatabase(dbName);
		influxDB.write(Point.measurement(measurement)
  					.tag("tag_user_guid", field)
  					.tag("tag_token_md5", token_md5)
  					.tag("tag_user_ip", user_ip)
  					.addField("user_guid", field)
  					.addField("token_md5", token_md5)
  					.addField("user_ip", user_ip)
  					.addField("count", Integer.valueOf(count))
  					.build());
		influxDB.close();
	}
	public void insertIndex(String measurement , String index , String count) throws Exception {
		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086");
		String dbName = "logforge";
		influxDB.query(new Query("CREATE DATABASE " + dbName));
		influxDB.setDatabase(dbName);
		influxDB.write(Point.measurement(measurement)
  					.tag("tag_index", index)
  					.addField("index", index)
  					.addField("count", Integer.valueOf(count))
  					.build());
		influxDB.close();
	}
	public void insertIP(String measurement , String ip , String count) throws Exception {
		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086");
		String dbName = "logforge";
		influxDB.query(new Query("CREATE DATABASE " + dbName));
		influxDB.setDatabase(dbName);
		influxDB.write(Point.measurement(measurement)
  					.tag("tag_ip", ip)
  					.addField("ip", ip)
  					.addField("count", Integer.valueOf(count))
  					.build());
		influxDB.close();
	}
	public void insertIPByte(String measurement , String ip , String body_bytes_sent) throws Exception {
		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086");
		String dbName = "logforge";
		influxDB.query(new Query("CREATE DATABASE " + dbName));
		influxDB.setDatabase(dbName);
		influxDB.write(Point.measurement(measurement)
  					.tag("tag_ip", ip)
  					.addField("ip", ip)
  					.addField("body_bytes_sent", Integer.valueOf(body_bytes_sent))
  					.build());
		influxDB.close();
	}
	public void insertMovie(String measurement , String field , String index , String count) throws Exception {
		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086");
		String dbName = "logforge";
		influxDB.query(new Query("CREATE DATABASE " + dbName));
		influxDB.setDatabase(dbName);
		influxDB.write(Point.measurement(measurement)
  					.tag("tag_movie", field)
  					.tag("tag_index", index)
  					.addField("movie", field)
  					.addField("index", index)
  					.addField("count", Integer.valueOf(count))
  					.build());
		influxDB.close();
	}
	public void insertOvod(String measurement , String field , String up_route , String token_md5 , String count) throws Exception {
		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086");
		String dbName = "logforge";
		influxDB.query(new Query("CREATE DATABASE " + dbName));
		influxDB.setDatabase(dbName);
		influxDB.write(Point.measurement(measurement)
  					.tag("tag_movie", field)
  					.tag("tag_up_route", up_route)
  					.tag("tag_token_md5", token_md5)
  					.addField("movie", field)
  					.addField("up_route", up_route)
  					.addField("token_md5", token_md5)
  					.addField("count", Integer.valueOf(count))
  					.build());
		influxDB.close();
	}
	public void insertToken(String measurement,String token_md5,String user_ip,String count) throws Exception {
		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086");
		String dbName = "logforge";
		influxDB.query(new Query("CREATE DATABASE " + dbName));
		influxDB.setDatabase(dbName);
		influxDB.write(Point.measurement(measurement)
  					.tag("tag_token_md5",token_md5)
  					.tag("tag_user_ip",user_ip)
  					.addField("token_md5",token_md5)
  					.addField("user_ip",user_ip)
  					.addField("count",Integer.valueOf(count))
  					.build());
		influxDB.close();
	}
	public void insertOrg(String measurement , String organization , String count) throws Exception {
		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086");
		String dbName = "logforge";
		influxDB.query(new Query("CREATE DATABASE " + dbName));
		influxDB.setDatabase(dbName);
		influxDB.write(Point.measurement(measurement)
  					.tag("tag_organization", organization)
  					.addField("organization", organization)
  					.addField("count", Integer.valueOf(count))
  					.build());
		influxDB.close();
	}
}
