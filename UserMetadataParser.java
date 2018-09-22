import org.apache.hadoop.io.Text;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class UserMetadataParser {
  
  private String userId;
  private String userName;
  private String userLocation;
  private String userLanguage;
  private String userScreenName;
  private String userFriends;
  private String userFollowers;

  public boolean parse(String record) throws ParseException { //using json simple to parse json file
      JSONParser jsonParser = new JSONParser();
      JSONObject jsonObject = (JSONObject) jsonParser.parse(record);
      JSONObject user = (JSONObject) jsonObject.get("user");
      userId=user.get("id").toString();
      userName=user.get("name").toString();
      userLocation = user.get("location").toString();
      userLanguage = user.get("lang").toString();
      userScreenName = user.get("screen_name").toString();
      userFriends = user.get("friends_count").toString();
      userFollowers= user.get("followers_count").toString();
      return true;     

  }
  
  public String getUserScreenName() {
	return userScreenName;
}

public String getUserLocation() {
	return userLocation;
}

public String getUserLanguage() {
	return userLanguage;
}


public String getUserFriends() {
	return userFriends;
}

public String getUserFollowers() {
	return userFollowers;
}

public boolean parse(Text record) throws ParseException {
    return parse(record.toString());
  }
  
  public String getUserId() {
    return userId;
  }

  public String getUserName() {
    return userName;
  }
  
}
