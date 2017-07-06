bag1 = load '/user/hive/warehouse/h1b_final' using PigStorage('\t') as (sno:int,casestatus:chararray,employername:chararray,socname:chararray,jobtitle:chararray,fulltime:chararray,prevailingwage:double,year:int,worksite:chararray,longitude:float,latitude:float);

bag2= filter bag1 by fulltime=='Y';

bag3= group bag2 by (year,jobtitle);

bag4= foreach bag3 generate group,AVG(bag2.prevailingwage) as avg;

bag5= order bag4 by avg desc;
dump bag5;






