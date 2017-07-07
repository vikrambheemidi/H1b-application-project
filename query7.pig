bag1 = load '/user/hive/warehouse/h1b_final' using PigStorage('\t') as (sno:int,casestatus:chararray,employername:chararray,socname:chararray,jobtitle:chararray,fulltime:chararray,prevailingwage:double,year:int,worksite:chararray,longitude:float,latitude:float);

bag2= group bag1 by year;

bag3= foreach bag2 generate group,COUNT(bag1.casestatus);
dump bag3;


