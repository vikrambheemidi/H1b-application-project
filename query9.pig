bag1 = load '/user/hive/warehouse/h1b_final' using PigStorage('\t') as (sno:int,casestatus:chararray,employername:chararray,socname:chararray,jobtitle:chararray,fulltime:chararray,prevailingwage:double,year:int,worksite:chararray,longitude:double,latitude:double);

bag2= group bag1 by employername;

bag3= foreach bag2 generate group,COUNT(bag1);

bag4= filter bag3 by $1>1000;

bag5= filter bag1 by casestatus=='CERTIFIED';
bag6= group bag5 by employername;
bag7= foreach bag6 generate group,COUNT(bag5);

bag8= filter bag1 by casestatus=='CERTIFIED-WITHDRAWN';
bag9= group bag8 by employername;
bag10= foreach bag9 generate group,COUNT(bag8);

bag11= join bag4 by group,bag7 by group,bag10 by group;

bag12= foreach bag11 generate $0,((($3+$5)*100)/$1) as per;
bag13= filter bag12 by $1>70;
bag14= order bag13 by $1 desc;
dump bag14;

