Program 5:

PriterClient and PrinterMaster are java programs

Compile the java programs by the following command:

javac -cp $CLASSPATH:. PrinterMaster.java
javac -cp $CLASSPATH:. PrinterClient.java

create jar file:
jar-cvf Vasanthi_Mudunuri_Program_5.jar *.class

Command to Execute:

java -cp $CLASSPATH:. PrinterMaster

This created the node PrinterQueue under znode vmudunu

java -cp $CLASSPATH:.  PrinterClient printer "print job 1"

This created ephimeral sequential nodes under the queue with data

arguments: znodename and data	