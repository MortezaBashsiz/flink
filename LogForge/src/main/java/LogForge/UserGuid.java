package logforge;

import java.util.Properties;
import java.lang.String;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import logforge.Log ;
import logforge.InfluxDBForge ;

public class UserGuid
{
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
		properties.setProperty("group.id", "UserGuidForge");

		DataStream<String> v_LogMinerOutStream = env.addSource(new FlinkKafkaConsumer<>("LogMinerOut", new SimpleStringSchema(), properties));
		DataStream<Log> v_Log = v_LogMinerOutStream.flatMap(new MakeLog());

		DataStream<Tuple5<String , String , String , String , Integer>> v_FieldCounterUserGuid = v_Log.flatMap(new FieldCounterUserGuid()).keyBy(1,2,3).timeWindow(Time.seconds(10)).sum(4);
		DataStream<String> v_FieldCounterUserGuid_string = v_FieldCounterUserGuid.flatMap(new TupleToStringUserGuid());

		env.execute("UserGuidForge Is Proccessing On Logs");
	}
	public static class MakeLog implements FlatMapFunction<String,Log> {
		@Override
		public void flatMap(String stream, Collector<Log> out) throws Exception {
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(stream);
			String token_md5 = (String) json.get("token_md5");
			String user_guid = (String) json.get("user_guid");
			String user_ip = (String) json.get("user_ip");
			Log v_Log = new Log();
			v_Log.token_md5 = token_md5 ;
			v_Log.user_guid = user_guid ;
			v_Log.user_ip = user_ip ;
			out.collect(v_Log);
		}
	}
	public static class FieldCounterUserGuid implements FlatMapFunction<Log,Tuple5<String,String,String,String,Integer>> {
		@Override
		public void flatMap(Log v_Log, Collector<Tuple5<String,String,String,String,Integer>> out) throws Exception {
			String token_md5 = (String) v_Log.token_md5 ;
			String user_guid = (String) v_Log.user_guid ;
			String user_ip = (String) v_Log.user_ip ;
			out.collect(new Tuple5<String,String,String,String,Integer>("user_guid",user_guid,token_md5,user_ip,1));
			
		}
	}
	public static class TupleToStringUserGuid implements FlatMapFunction<Tuple5<String,String,String,String,Integer>,String> {
		@Override
		public void flatMap(Tuple5<String,String,String,String,Integer> v_Tuple5,  Collector<String> out) throws Exception {
			String measurement = v_Tuple5.getField(0) ;
			String field = v_Tuple5.getField(1) ;
			String token_md5 = v_Tuple5.getField(2) ;
			String user_ip = v_Tuple5.getField(2) ;
			String count = Integer.toString(v_Tuple5.getField(4)) ;
			CInfluxDB v_InfluxDB = new CInfluxDB() ;
		 	v_InfluxDB.insertUserGuid(measurement,field,token_md5,user_ip,count) ;
		}
	}
	public static class CInfluxDB {
		public void insertUserGuid(String measurement , String field , String token_md5 , String user_ip , String count) throws Exception {
			InfluxDBForge influxdb = new InfluxDBForge();
			influxdb.insertUserGuid(measurement,field,token_md5,user_ip,count);
		}
	}
} 