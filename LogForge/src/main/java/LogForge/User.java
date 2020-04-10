package logforge;

import java.util.Properties;
import java.lang.String;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import logforge.Log ;
import logforge.InfluxDBForge ;

public class User
{
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
		properties.setProperty("group.id", "UserForge");

		DataStream<String> v_LogMinerOutStream = env.addSource(new FlinkKafkaConsumer<>("LogMinerOut", new SimpleStringSchema(), properties));
		DataStream<Log> v_Log = v_LogMinerOutStream.flatMap(new MakeLog());

		DataStream<Tuple4<String , String , String , Integer>> v_FieldCounterUser = v_Log.flatMap(new FieldCounterUser()).keyBy(1,2).timeWindow(Time.seconds(10)).sum(3);
		DataStream<String> v_FieldCounterUser_string = v_FieldCounterUser.flatMap(new TupleToStringUser());

		DataStream<Tuple3<String , String , Integer>> v_FieldCounterUserStatus = v_Log.flatMap(new FieldCounterUserStatus()).keyBy(1).timeWindow(Time.seconds(10)).sum(2);
		DataStream<String> v_FieldCounterUserStatus_string = v_FieldCounterUserStatus.flatMap(new TupleToStringUserStatus());

		env.execute("UserForge Is Proccessing On Logs");
	}
	public static class MakeLog implements FlatMapFunction<String,Log> {
		@Override
		public void flatMap(String stream, Collector<Log> out) throws Exception {
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(stream);
			String index = (String) json.get("index");
			String token_md5 = (String) json.get("token_md5");
			String status = (String) json.get("status");
			Log v_Log = new Log();
			v_Log.index = index ;
			v_Log.token_md5 = token_md5 ;
			v_Log.status = status ;
			out.collect(v_Log);
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
	public static class FieldCounterUserStatus implements FlatMapFunction<Log,Tuple3<String,String,Integer>> {
		@Override
		public void flatMap(Log v_Log, Collector<Tuple3<String,String,Integer>> out) throws Exception {
			String status = (String) v_Log.status ;
			out.collect(new Tuple3<String,String,Integer>("status",status,1));
			
		}
	}
	public static class TupleToStringUserStatus implements FlatMapFunction<Tuple3<String,String,Integer>,String> {
		@Override
		public void flatMap(Tuple3<String,String,Integer> v_Tuple3,  Collector<String> out) throws Exception {
			String measurement = v_Tuple3.getField(0) ;
			String field = v_Tuple3.getField(1) ;
			String count = Integer.toString(v_Tuple3.getField(2)) ;
			CInfluxDB v_InfluxDB = new CInfluxDB() ;
		 	v_InfluxDB.insertUserStatus(measurement,field,count) ;
		}
	}
	public static class CInfluxDB {
		public void insertUser(String measurement , String field , String index , String count) throws Exception {
			InfluxDBForge influxdb = new InfluxDBForge();
			influxdb.insertUser(measurement,field,index,count);
		}
		public void insertUserStatus(String measurement , String field , String count) throws Exception {
			InfluxDBForge influxdb = new InfluxDBForge();
			influxdb.insertUserStatus(measurement,field,count);
		}
	}
} 