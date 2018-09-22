import java.io.*;
import java.util.*;

import org.apache.hadoop.io.IOUtils;
import org.json.simple.parser.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class UserMetadata {
  
  private Map<String, List<String>> userIdToName = new HashMap<String, List<String>>();
  public void initialize(String file_path, boolean isHdfsPath, String HdfsMasterURL) throws IOException {
	  BufferedReader in = null;
	  try {
		  if (isHdfsPath){
			  Path pt=new Path(file_path);
			  Configuration config = new Configuration();
		      config.set("fs.defaultFS", HdfsMasterURL);
              FileSystem fs = FileSystem.get(config);
              in=new BufferedReader(new InputStreamReader(fs.open(pt)));
		  }else{
			  in = new BufferedReader(new InputStreamReader(new FileInputStream(new File(file_path))));
		  }
		  UserMetadataParser parser = new UserMetadataParser();
		  String line;
		  while ((line = in.readLine()) != null) {
			  try {
				if (parser.parse(line)) { //creating array list to save as value in the Map
                                          List<String> userInfo=new ArrayList<String>();
					  userInfo.add(0,parser.getUserName());
					  userInfo.add(1,parser.getUserLocation());
					  userInfo.add(2,parser.getUserLanguage());
					  userInfo.add(3,parser.getUserScreenName());
					  userInfo.add(4,parser.getUserFriends());
					  userInfo.add(5,parser.getUserFollowers());
					  userIdToName.put(parser.getUserId(),userInfo);
				  }
			} catch (ParseException e) {
				e.printStackTrace();
			}
		  }
	  } finally {
		  IOUtils.closeStream(in);
	  }
  }

  public Map<String, List<String>> getUserMap() {
    return (Map<String, List<String>>) Collections.unmodifiableMap(userIdToName);
  }
  
}
