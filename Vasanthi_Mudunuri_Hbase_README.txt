Table user:
Coulmn Family:info
Coulmns:userid,name,location,language,screenname,frinedscount and followerscount

Table tweets:
Coulmn Family:Tweets
Columns:id,createdat,timestamp,text,favorited,favoritecount,retweeted,userid,name,location

To prase the Json input file JSON.simple parser is been used

1.The jar file json-simple-1.1 should be added to the classpath before executing the program
2.export CLASSPATH=(add all the jars here):/path/to/json-simple-1.1.jar

Create two HTables for user and tweets by issuing following commands in HBase shell client

create 'vmudunu:user', {NAME => 'info'}
create 'vmudunu:tweets', {NAME => 'Tweets’}
Steps & order to compile programs to load user data into HTable

Comiple all the below java programs by the following command:

javac -cp $CLASSPATH:. UserMetadata.java
javac -cp $CLASSPATH:. UserMetadataParser.java
javac -cp $CLASSPATH:. TwitterUserQuery.java
javac -cp $CLASSPATH:. UserImporter.java

Importing Data into user HTable
command:java -cp $CLASSPATH:. UserImporter hdfs hdfs://hadoop1:9000/CS5433/PA1/Data/FlumeData.1441733310094 vmudunu:user hdfs://hadoop1:9000

Open Hbase Shell and check if the rows have been inserted by typing "count 'vmudunu:user'" on the CLI. If data has been insterted the count should not be zero.

Steps & order to compile java programs to load user tweets data into HTable

javac -cp $CLASSPATH:. TweetsParser.java
javac -cp $CLASSPATH:. TwitterQuery.java
javac -cp $CLASSPATH:. TweetsImporter.java
Bundle all the java files into a JAR file

jar -cvf Vasanthi_Mudunuri_Program_4.jar *.class

To import usertweets data issue the following command in CLI
command:java -cp $CLASSPATH:. TweetsImporter hdfs://hadoop1:9000/CS5433/PA1/Data/FlumeData.1441733310094 vmudunu:tweets

Open Hbase Shell and check if the rows have been inserted by typing "count 'vmudunu:tweets'" on the CLI. If data has been insterted the count should not be zero.

To Query User & Tweets data use the following commands
hbase -cp $CLASSPATH:. TwitterUserQuery vmudunu:user 233901036
arguments: tablename and userID
hbase -cp $CLASSPATH:. TwitterQuery vmudunu:tweets 3404410572 5
arguments: tablename,userID and count