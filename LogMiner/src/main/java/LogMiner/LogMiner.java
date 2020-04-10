package logminer;

import java.util.Properties;
import java.lang.String;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import logminer.Log;
import logminer.GeoLocation;


public class LogMiner 
{
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
		properties.setProperty("group.id", "LogMiner");

		FlinkKafkaProducer<String> kafka_producer = new FlinkKafkaProducer<String>("127.0.0.1:9092","LogMinerOut",new SimpleStringSchema());
		kafka_producer.setWriteTimestampToKafka(true);

		DataStream<String> v_LogMinerInStream = env.addSource(new FlinkKafkaConsumer<>("LogMinerIn", new SimpleStringSchema(), properties));
		DataStream<Log> v_Log = v_LogMinerInStream.flatMap(new MakeLog());
		DataStream<String> v_StringLogMinerOut = v_Log.flatMap(new LogToString());

		v_StringLogMinerOut.addSink(kafka_producer);

		env.execute("LogMiner Is Mining The Logs");
	}

	public static class MakeLog implements FlatMapFunction<String,Log> {
		@Override
		public void flatMap(String stream, Collector<Log> out) throws Exception {
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(stream);
			JSONObject user_info = (JSONObject) json.get("user_info");

			GeoLocation clsGeoLocation = new GeoLocation();
			JSONObject jsonCountry = clsGeoLocation.GetCountryJson();

			boolean is_index = false ;
			boolean is_first_seg = false ;
			boolean is_video_seg = false ;

			String up_route = "EMPTY";
			String movie_id = "EMPTY";
			String movie_name = "EMPTY";
			String request = "EMPTY";
			String movie_segment = "EMPTY";
			String index = "FALSE";
			String host = "EMPTY";
			String https = "EMPTY";
			String status = "EMPTY";
			String user_guid = "EMPTY";
			String user_ip = "EMPTY";
			String body_bytes_sent = "EMPTY";
			String city = "EMPTY";
			String country = "EMPTY";
			String latitude = "EMPTY";
			String longitude = "EMPTY";
			String organization = "EMPTY";
			String token_md5 = "EMPTY";

			up_route = (String) json.get("up_route");
			if ( up_route == null )
			{
				up_route = "NULL" ;
			}
			movie_id = (String) json.get("movie_id");
			if ( movie_id == null )
			{
				movie_id = "NULL" ;
			}
			movie_name = (String) json.get("movie_name");
			if ( movie_name == null )
			{
				movie_name = "NULL" ;
			}
			request = (String) json.get("request");
			if ( request == null )
			{
				request = "NULL" ;
			}
			movie_segment = (String) json.get("movie_segment");

			if ( movie_segment != null )
			{
				is_index = movie_segment.indexOf("index-v1-a1.m3u8") >= 0;
				if (is_index)
				{
					index = "INDEX" ;
				}
				is_first_seg = movie_segment.indexOf("seg-1-v1-a1.ts") >= 0;
				if (is_first_seg)
				{
					index = "FIRST_SEG" ;
				}
				is_video_seg = movie_segment.indexOf("v1-a1.ts") >= 0;
				if ((!is_first_seg) && is_video_seg)
				{
					index = "VIDEO_SEG" ;
				}
			}

			host = (String) json.get("host");
			if ( host == null )
			{
				host = "NULL" ;
			}
			https = (String) json.get("https");
			if ( https == null )
			{
				https = "NULL" ;
			}
			status = (String) user_info.get("status");
			if ( status == null )
			{
				status = "NULL" ;
			}
			user_guid = (String) user_info.get("user_guid");
			if ( https == null )
			{
				https = "NULL" ;
			}
			user_ip = (String) user_info.get("user_ip");
			if ( user_ip == null )
			{
				user_ip = "NULL" ;
			}
			body_bytes_sent = String.valueOf(json.get("body_bytes_sent"));
			if ( body_bytes_sent == null )
			{
				body_bytes_sent = "NULL" ;
			}
			city = (String) json.get("city");
			if ( city == null )
			{
				city = "NULL" ;
			}
			country = (String) json.get("country");
			if ( country != null )
			{
				JSONObject country_info = (JSONObject) jsonCountry.get(country);
				latitude = String.valueOf(country_info.get("latitude"));
				longitude = String.valueOf(country_info.get("longitude"));
			}
			organization = (String) json.get("organization");
			if ( organization == null )
			{
				organization = "NULL" ;
			}
			token_md5 = (String) json.get("token_md5");
			if ( token_md5 == null )
			{
				token_md5 = "NULL" ;
			}
			
			Log v_Log = new Log();

			v_Log.up_route = up_route ;
			v_Log.movie_id = movie_id ;
			v_Log.movie_name = movie_name ;
			v_Log.request = request ;
			v_Log.movie_segment = movie_segment ;
			v_Log.index = index ;
			v_Log.movie_segment = movie_segment ;
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

	public static class LogToString implements FlatMapFunction<Log,String> {
		@Override
		public void flatMap(Log v_Log, Collector<String> out) throws Exception {
			String up_route = (String) v_Log.up_route ; 
			String movie_id = (String) v_Log.movie_id ; 
			String movie_name = (String) v_Log.movie_name ; 
			String request = (String) v_Log.request ;
			String index = (String) v_Log.index ;
			String movie_segment = (String) v_Log.movie_segment ;
			String host = (String) v_Log.host ; 
			String https = (String) v_Log.https ; 
			String status = (String) v_Log.status ; 
			String user_guid = (String) v_Log.user_guid ; 
			String user_ip = (String) v_Log.user_ip ; 
			String body_bytes_sent = (String) v_Log.body_bytes_sent;
			String city = (String) v_Log.city;
			String country = (String) v_Log.country;
			String latitude = (String) v_Log.latitude;
			String longitude = (String) v_Log.longitude;
			String organization = (String) v_Log.organization;
			String token_md5 = (String) v_Log.token_md5;
			out.collect("{ \"up_route\" : \""+up_route+"\" , \"movie_id\" : \""+movie_id+"\" , \"movie_name\" : \""+movie_name+"\" , \"request\" : \""+request+"\" , \"index\" : \""+index+"\" , \"movie_segment\" : \""+movie_segment+"\" , \"user_guid\" : \""+ user_guid + "\" , \"user_ip\" : \""+ user_ip + "\" , \"host\" : \""+ host + "\" , \"https\" : \""+ https + "\" ,  \"status\" : \""+ status + "\" , \"body_bytes_sent\" : \""+body_bytes_sent+"\" , \"city\" : \""+city+"\" , \"country\" : \""+country+"\" , \"latitude\" : \""+latitude+"\" , \"longitude\" : \""+longitude+"\" , \"organization\" : \""+organization+"\" , \"token_md5\" : \""+token_md5+"\" }");
		}
	}
}