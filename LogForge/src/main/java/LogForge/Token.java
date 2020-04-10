package logforge;

import java.util.Properties;
import java.lang.String;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import logforge.Log ;
import logforge.InfluxDBForge ;

public class Token 
{
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
		properties.setProperty("group.id", "CountryForge");
		DataStream<String> v_LogMinerOutStream = env.addSource(new FlinkKafkaConsumer<>("LogMinerOut", new SimpleStringSchema(), properties));
		DataStream<Log> v_Log = v_LogMinerOutStream.flatMap(new MakeLog());
		DataStream<Tuple4<String , String , String , Integer>> v_FieldTokenIP = v_Log.flatMap(new FieldTokenIP()).keyBy(1,2).timeWindow(Time.seconds(10)).sum(3);
		DataStream<String> v_FieldTokenIP_string = v_FieldTokenIP.flatMap(new TupleToStringToken());
		env.execute("TokenForge Is Proccessing On Logs");
	}
	public static class MakeLog implements FlatMapFunction<String,Log> {
		@Override
		public void flatMap(String stream, Collector<Log> out) throws Exception {
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(stream);
			String user_ip = (String) json.get("user_ip");
			String token_md5 = (String) json.get("token_md5");
			Log v_Log = new Log();
			v_Log.user_ip = user_ip ;
			v_Log.token_md5 = token_md5 ;
			out.collect(v_Log);
		}
	}
	public static class FieldTokenIP implements FlatMapFunction<Log,Tuple4<String,String,String,Integer>> {
		@Override
		public void flatMap(Log v_Log, Collector<Tuple4<String,String,String,Integer>> out) throws Exception {
			String user_ip = (String) v_Log.user_ip ;
			String token_md5 = (String) v_Log.token_md5 ;
			out.collect(new Tuple4<String,String,String,Integer>("token",token_md5,user_ip,1));
		}
	}
	public static class TupleToStringToken implements FlatMapFunction<Tuple4<String,String,String,Integer>,String> {
		@Override
		public void flatMap(Tuple4<String,String,String,Integer> v_Tuple4,  Collector<String> out) throws Exception {
			String measurement = v_Tuple4.getField(0) ;
			String token_md5 = v_Tuple4.getField(1) ;
			String user_ip = v_Tuple4.getField(2) ;
			String count = Integer.toString(v_Tuple4.getField(3)) ;
			CInfluxDB v_InfluxDB = new CInfluxDB();
		 	v_InfluxDB.insertToken(measurement,token_md5,user_ip,count);
		}
	}
	public static class CInfluxDB {
		public void insertToken(String measurement,String token_md5,String user_ip,String count) throws Exception {
			InfluxDBForge influxdb = new InfluxDBForge();
			influxdb.insertToken(measurement,token_md5,user_ip,count);
		}
	}
} 