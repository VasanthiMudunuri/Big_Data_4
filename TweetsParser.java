import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.Text;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class TweetsParser {

	private String user__id;
	private String user__name;
	private String user__location;
	private String id;
	private String created_at;
	private String timestamp_ms;
	private String text;
	private String favorited;
	private String favorite_count;
	private String retweeted;

	public void parse(String record) throws ParseException { //parsing json file using json simple
		JSONParser jsonParser = new JSONParser(); 
                JSONObject jsonObject = (JSONObject) jsonParser.parse(record);
                id=jsonObject.get("id").toString();
		created_at=jsonObject.get("created_at").toString();
		timestamp_ms=jsonObject.get("timestamp_ms").toString();
		text=jsonObject.get("text").toString();
		favorited=jsonObject.get("favorited").toString();
		favorite_count=jsonObject.get("favorite_count").toString();
		retweeted=jsonObject.get("retweeted").toString();
		JSONObject obj=(JSONObject) jsonObject.get("user");
		user__id = obj.get("id").toString();
		user__name=obj.get("name").toString();
		user__location=obj.get("location").toString();
	}
    
	public void setCreated_at(String created_at) throws Exception {
		DateFormat originalFormat = new SimpleDateFormat("EEE MMM dd hh:MM:ss z YYYY", Locale.ENGLISH);
		DateFormat targetFormat = new SimpleDateFormat("hh/MM/ss");
		Date date = originalFormat.parse(created_at);
		String formattedDate = targetFormat.format(created_at);
		this.created_at = formattedDate;
	}

	public void parse(Text record) throws ParseException {
		parse(record.toString());
	}

	public String getUser__id() {
		return user__id;
	}

	public String getUser__name() {
		return user__name;
	}

	public String getUser__location() {
		return user__location;
	}

	public String getId() {
		return id;
	}

	public String getCreated_at() {
		return created_at;
	}

	public String getTimestamp_ms() {
		return timestamp_ms;
	}

	public String getText() {
		return text;
	}

	public String getFavorited() {
		return favorited;
	}

	public String getFavorite_count() {
		return favorite_count;
	}

	public String getRetweeted() {
		return retweeted;
	}
}

