To run the spark jobs written in scala:
1.copy Vasanthi_Mudunuri_Program_1.sbt to the present working directory
2.Create path in present working directory as src/main/scala/ and copy Vasanthi_Mudunuri_Program_1.scala,
Vasanthi_Mudunuri_Program_2.scala,Vasanthi_Mudunuri_Program_3.scala programs into it
3.From the present working directory type : sbt package
4.After succesfull completion this creates project and target folders under present working directory
5.The target folder contains scala-2.10 directory which contains the jar file vasanthi_mudunuri_program_2.10-1.0.jar 
6.Now give the command below as specified for each program to run the spark job.

Note: here both the input and output paths are in hdfs


Program 1:
spark-submit --class Vasanthi_Mudunuri_Program_1 target/scala-2.10/vasanthi_mudunuri_program_2.10-1.0.jar /CS5433/PA2/ratings.dat /vmudunu/program1

arguments: inputpath,ouputpath

Program 2:
spark-submit --class Vasanthi_Mudunuri_Program_2 target/scala-2.10/vasanthi_mudunuri_program_2.10-1.0.jar /CS5433/PA2/ratings.dat /vmudunu/program2 2796

arguments: inputpath,outputpath,movieID

Program 3:

spark-submit --class Vasanthi_Mudunuri_Program_3  target/scala-2.10/vasanthi_mudunuri_program_2.10-1.0.jar /CS5433/PA3/ACS/ACS_15_5YR_S0101_with_ann.csv /CS5433/PA3/CFS/CFS_2012_00A01_with_ann.csv /vmudunu/program3

arguments: two inputpaths,outputpath
Output is generated as a sequence file which can be viewed by the following command:
hdfs dfs -text /vmudunu/program3
