package logforge;

import java.util.Properties;
import java.lang.String;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import logforge.Log ;
import logforge.InfluxDBForge ;

public class LogForge 
{
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
		properties.setProperty("group.id", "LogForge");

		DataStream<String> v_LogMinerOutStream = env.addSource(new FlinkKafkaConsumer<>("LogMinerOut", new SimpleStringSchema(), properties));
		DataStream<Log> v_Log = v_LogMinerOutStream.flatMap(new MakeLog());

		DataStream<Tuple6<String , String , String , Integer , String , String>> v_FieldCounterCountry = v_Log.flatMap(new FieldCounterCountry()).keyBy(1,2).timeWindow(Time.seconds(10)).sum(3);
		DataStream<String> v_FieldCounterCountry_string = v_FieldCounterCountry.flatMap(new TupleToStringCountry());

		DataStream<Tuple4<String , String , String , Integer>> v_FieldCounterUser = v_Log.flatMap(new FieldCounterUser()).keyBy(1,2).timeWindow(Time.seconds(10)).sum(3);
		DataStream<String> v_FieldCounterUser_string = v_FieldCounterUser.flatMap(new TupleToStringUser());

		DataStream<Tuple3<String , String , Integer>> v_FieldCounterIndex = v_Log.flatMap(new FieldCounterIndex()).keyBy(1).timeWindow(Time.seconds(10)).sum(2);
		DataStream<String> v_FieldCounterIndex_string = v_FieldCounterIndex.flatMap(new TupleToStringIndex());

		DataStream<Tuple3<String , String , Integer>> v_FieldCounterIP = v_Log.flatMap(new FieldCounterIP()).keyBy(1).timeWindow(Time.seconds(10)).sum(2);
		DataStream<String> v_FieldCounterIP_string = v_FieldCounterIP.flatMap(new TupleToStringIP());

		DataStream<Tuple3<String , String , Integer>> v_FieldIPByte = v_Log.flatMap(new FieldIPByte()).keyBy(1).timeWindow(Time.seconds(10)).sum(2);
		DataStream<String> v_FieldIPByte_string = v_FieldIPByte.flatMap(new TupleToStringIPByte());

		DataStream<Tuple4<String , String , String , Integer>> v_FieldCounterMovie = v_Log.flatMap(new FieldCounterMovie()).keyBy(1,2).timeWindow(Time.seconds(10)).sum(3);
		DataStream<String> v_FieldCounterMovie_string = v_FieldCounterMovie.flatMap(new TupleToStringMovie());

		DataStream<Tuple5<String , String , String , String , Integer>> v_FieldCounterOvod = v_Log.flatMap(new FieldCounterOvod()).keyBy(1,2,3).timeWindow(Time.seconds(10)).sum(4);
		DataStream<String> v_FieldCounterOvod_string = v_FieldCounterOvod.flatMap(new TupleToStringOvod());

		env.execute("LogForge Is Proccessing On Logs");
	}

	public static class MakeLog implements FlatMapFunction<String,Log> {
		@Override
		public void flatMap(String stream, Collector<Log> out) throws Exception {
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(stream);

			String up_route = (String) json.get("up_route");
			String movie_id = (String) json.get("movie_id");
			String movie_name = (String) json.get("movie_name");
			String request = (String) json.get("request");
			String movie_segment = (String) json.get("movie_segment");
			String index = (String) json.get("index");
			String host = (String) json.get("host");
			String https = (String) json.get("https");
			String status = String.valueOf(json.get("status"));
			String user_guid = (String) json.get("user_guid");
			String user_ip = (String) json.get("user_ip");
			String body_bytes_sent = String.valueOf(json.get("body_bytes_sent"));
			String city = (String) json.get("city");
			String country = (String) json.get("country");
			String latitude = String.valueOf(json.get("latitude"));
			String longitude = String.valueOf(json.get("longitude"));
			String organization = (String) json.get("organization");
			String token_md5 = (String) json.get("token_md5");
			
			Log v_Log = new Log();

			v_Log.up_route = up_route ;
			v_Log.movie_id = movie_id ;
			v_Log.movie_name = movie_name ;
			v_Log.request = request ;
			v_Log.movie_segment = movie_segment ;
			v_Log.index = index ;
			v_Log.host = host ;
			v_Log.https = https ;
			v_Log.status = status ;
			v_Log.user_guid = user_guid ;
			v_Log.user_ip = user_ip ;
			v_Log.body_bytes_sent = body_bytes_sent ;
			v_Log.city = city ;
			v_Log.country = country ;
			v_Log.latitude = latitude ;
			v_Log.longitude = longitude ;
			v_Log.organization = organization ;
			v_Log.token_md5 = token_md5 ;

			out.collect(v_Log);
		}
	}

	public static class FieldCounterCountry implements FlatMapFunction<Log,Tuple6<String,String,String,Integer,String,String>> {
		@Override
		public void flatMap(Log v_Log, Collector<Tuple6<String,String,String,Integer,String,String>> out) throws Exception {
			String latitude = (String) v_Log.latitude ;
			String longitude = (String) v_Log.longitude ;
			String country = (String) v_Log.country ;
			String token_md5 = (String) v_Log.token_md5 ;
			if (!latitude.equals("EMPTY"))
			{
				out.collect(new Tuple6<String,String,String,Integer,String,String>("country",country,token_md5,1,latitude,longitude));
			}
		}
	}

	public static class FieldCounterUser implements FlatMapFunction<Log,Tuple4<String,String,String,Integer>> {
		@Override
		public void flatMap(Log v_Log, Collector<Tuple4<String,String,String,Integer>> out) throws Exception {
			String token_md5 = (String) v_Log.token_md5 ;
			String index = (String) v_Log.index ;
			out.collect(new Tuple4<String,String,String,Integer>("user",token_md5,index,1));
			
		}
	}

	public static class FieldCounterIndex implements FlatMapFunction<Log,Tuple3<String,String,Integer>> {
		@Override
		public void flatMap(Log v_Log, Collector<Tuple3<String,String,Integer>> out) throws Exception {
			String index = (String) v_Log.index ;
			out.collect(new Tuple3<String,String,Integer>("index",index,1));
		}
	}

	public static class FieldCounterIP implements FlatMapFunction<Log,Tuple3<String,String,Integer>> {
		@Override
		public void flatMap(Log v_Log, Collector<Tuple3<String,String,Integer>> out) throws Exception {
			String user_ip = (String) v_Log.user_ip ;
			out.collect(new Tuple3<String,String,Integer>("user_ip",user_ip,1));
		}
	}

	public static class FieldIPByte implements FlatMapFunction<Log,Tuple3<String,String,Integer>> {
		@Override
		public void flatMap(Log v_Log, Collector<Tuple3<String,String,Integer>> out) throws Exception {
			String user_ip = (String) v_Log.user_ip ;
			String body_bytes_sent = (String) v_Log.body_bytes_sent ;
			out.collect(new Tuple3<String,String,Integer>("ip_byte",user_ip,Integer.valueOf(body_bytes_sent)));
		}
	}

	public static class FieldCounterMovie implements FlatMapFunction<Log,Tuple4<String,String,String,Integer>> {
		@Override
		public void flatMap(Log v_Log, Collector<Tuple4<String,String,String,Integer>> out) throws Exception {
			String movie_name = (String) v_Log.movie_name ;
			String index = (String) v_Log.index ;
			out.collect(new Tuple4<String,String,String,Integer>("movie",movie_name,index,1));
			
		}
	}

	public static class FieldCounterOvod implements FlatMapFunction<Log,Tuple5<String,String,String,String,Integer>> {
		@Override
		public void flatMap(Log v_Log, Collector<Tuple5<String,String,String,String,Integer>> out) throws Exception {
			String movie_name = (String) v_Log.movie_name ;
			String up_route = (String) v_Log.up_route ;
			String token_md5 = (String) v_Log.token_md5 ;
			out.collect(new Tuple5<String,String,String,String,Integer>("ovod",movie_name,up_route,token_md5,1));
		}
	}

	public static class TupleToStringCountry implements FlatMapFunction<Tuple6<String , String , String , Integer , String , String>,String> {
		@Override
		public void flatMap(Tuple6<String,String,String,Integer,String,String> v_Tuple6,  Collector<String> out) throws Exception {
			String measurement = v_Tuple6.getField(0) ;
			String field = v_Tuple6.getField(1) ;
			String token_md5 = v_Tuple6.getField(1) ;
			String count = Integer.toString(v_Tuple6.getField(3)) ;
			String latitude = v_Tuple6.getField(4);
			String longitude = v_Tuple6.getField(5);
			CInfluxDB v_InfluxDB = new CInfluxDB();
		 	v_InfluxDB.insertCountry(measurement,field,token_md5,count,latitude,longitude);
		}
	}

	public static class TupleToStringUser implements FlatMapFunction<Tuple4<String,String,String,Integer>,String> {
		@Override
		public void flatMap(Tuple4<String,String,String,Integer> v_Tuple4,  Collector<String> out) throws Exception {
			String measurement = v_Tuple4.getField(0) ;
			String field = v_Tuple4.getField(1) ;
			String index = v_Tuple4.getField(2) ;
			String count = Integer.toString(v_Tuple4.getField(3)) ;
			CInfluxDB v_InfluxDB = new CInfluxDB() ;
		 	v_InfluxDB.insertUser(measurement,field,index,count) ;
		}
	}

	public static class TupleToStringIndex implements FlatMapFunction<Tuple3<String,String,Integer>,String> {
		@Override
		public void flatMap(Tuple3<String,String,Integer> v_Tuple3,  Collector<String> out) throws Exception {
			String measurement = v_Tuple3.getField(0) ;
			String index = v_Tuple3.getField(1) ;
			String count = Integer.toString(v_Tuple3.getField(2)) ;
			CInfluxDB v_InfluxDB = new CInfluxDB();
		 	v_InfluxDB.insertIndex(measurement,index,count);
		}
	}

	public static class TupleToStringIP implements FlatMapFunction<Tuple3<String,String,Integer>,String> {
		@Override
		public void flatMap(Tuple3<String,String,Integer> v_Tuple3,  Collector<String> out) throws Exception {
			String measurement = v_Tuple3.getField(0);
			String ip = v_Tuple3.getField(1);
			String count = Integer.toString(v_Tuple3.getField(2));
			CInfluxDB v_InfluxDB = new CInfluxDB();
		 	v_InfluxDB.insertIP(measurement,ip,count);
		}
	}

	public static class TupleToStringIPByte implements FlatMapFunction<Tuple3<String,String,Integer>,String> {
		@Override
		public void flatMap(Tuple3<String,String,Integer> v_Tuple3,  Collector<String> out) throws Exception {
			String measurement = v_Tuple3.getField(0);
			String ip = v_Tuple3.getField(1);
			String body_bytes_sent = Integer.toString(v_Tuple3.getField(2));
			CInfluxDB v_InfluxDB = new CInfluxDB();
		 	v_InfluxDB.insertIPByte(measurement,ip,body_bytes_sent);
		}
	}

	public static class TupleToStringMovie implements FlatMapFunction<Tuple4<String,String,String,Integer>,String> {
		@Override
		public void flatMap(Tuple4<String,String,String,Integer> v_Tuple4,  Collector<String> out) throws Exception {
			String measurement = v_Tuple4.getField(0) ;
			String field = v_Tuple4.getField(1) ;
			String index = v_Tuple4.getField(2) ;
			String count = Integer.toString(v_Tuple4.getField(3)) ;
			CInfluxDB v_InfluxDB = new CInfluxDB() ;
		 	v_InfluxDB.insertMovie(measurement,field,index,count) ;
		}
	}

	public static class TupleToStringOvod implements FlatMapFunction<Tuple5<String,String,String,String,Integer>,String> {
		@Override
		public void flatMap(Tuple5<String,String,String,String,Integer> v_Tuple5,  Collector<String> out) throws Exception {
			String measurement = v_Tuple5.getField(0) ;
			String field = v_Tuple5.getField(1) ;
			String up_route = v_Tuple5.getField(2) ;
			String token_md5 = v_Tuple5.getField(3) ;
			String count = Integer.toString(v_Tuple5.getField(4)) ;
			CInfluxDB v_InfluxDB = new CInfluxDB() ;
		 	v_InfluxDB.insertOvod(measurement,field,up_route,token_md5,count) ;
		}
	}

	public static class CInfluxDB {
		public void insertCountry(String measurement,String field,String token_md5,String count,String latitude,String longitude) throws Exception {
			InfluxDBForge influxdb = new InfluxDBForge();
			influxdb.insertCountry(measurement,field,token_md5,count,latitude,longitude);
		}
		public void insertUser(String measurement , String field , String index , String count) throws Exception {
			InfluxDBForge influxdb = new InfluxDBForge();
			influxdb.insertUser(measurement,field,index,count);
		}
		public void insertIndex(String measurement , String index , String count) throws Exception {
			InfluxDBForge influxdb = new InfluxDBForge();
			influxdb.insertIndex(measurement,index,count);
		}
		public void insertIP(String measurement , String ip , String count) throws Exception {
			InfluxDBForge influxdb = new InfluxDBForge();
			influxdb.insertIP(measurement,ip,count);
		}
		public void insertIPByte(String measurement , String ip , String body_bytes_sent) throws Exception {
			InfluxDBForge influxdb = new InfluxDBForge();
			influxdb.insertIP(measurement,ip,body_bytes_sent);
		}
		public void insertMovie(String measurement , String field , String index , String count) throws Exception {
			InfluxDBForge influxdb = new InfluxDBForge();
			influxdb.insertMovie(measurement,field,index,count);
		}
		public void insertOvod(String measurement , String field , String up_route , String token_md5 , String count) throws Exception {
			InfluxDBForge influxdb = new InfluxDBForge();
			influxdb.insertOvod(measurement,field,up_route,token_md5,count);
		}
	}
} 