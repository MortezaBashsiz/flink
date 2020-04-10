package logforge;

import java.util.Properties;
import java.lang.String;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import logforge.Log ;
import logforge.InfluxDBForge ;

public class Movie
{
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
		properties.setProperty("group.id", "MovieForge");
		DataStream<String> v_LogMinerOutStream = env.addSource(new FlinkKafkaConsumer<>("LogMinerOut", new SimpleStringSchema(), properties));
		DataStream<Log> v_Log = v_LogMinerOutStream.flatMap(new MakeLog());
		DataStream<Tuple4<String , String , String , Integer>> v_FieldCounterMovie = v_Log.flatMap(new FieldCounterMovie()).keyBy(1,2).timeWindow(Time.seconds(10)).sum(3);
		DataStream<String> v_FieldCounterMovie_string = v_FieldCounterMovie.flatMap(new TupleToStringMovie());
		DataStream<Tuple5<String , String , String , String , Integer>> v_FieldCounterOvod = v_Log.flatMap(new FieldCounterOvod()).keyBy(1,2,3).timeWindow(Time.seconds(10)).sum(4);
		DataStream<String> v_FieldCounterOvod_string = v_FieldCounterOvod.flatMap(new TupleToStringOvod());
		env.execute("MovieForge Is Proccessing On Logs");
	}
	public static class MakeLog implements FlatMapFunction<String,Log> {
		@Override
		public void flatMap(String stream, Collector<Log> out) throws Exception {
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(stream);
			String up_route = (String) json.get("up_route");
			String movie_name = (String) json.get("movie_name");
			String index = (String) json.get("index");
			String token_md5 = (String) json.get("token_md5");
			Log v_Log = new Log();
			v_Log.up_route = up_route ;
			v_Log.movie_name = movie_name ;
			v_Log.index = index ;
			v_Log.token_md5 = token_md5 ;
			out.collect(v_Log);
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