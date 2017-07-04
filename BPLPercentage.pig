REGISTER /usr/local/pig/lib/piggybank.jar;
DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath();

A =  LOAD '/flume_import/StatewiseDistrictwisePhysicalProgress.xml' using org.apache.pig.piggybank.storage.XMLLoader('row') as (x:chararray);

B = FOREACH A GENERATE XPath(x, 'row/District_Name') AS District, (long)XPath(x, 'row/Project_Objectives_IHHL_BPL') AS BPL,
(long)XPath(x, 'row/Project_Performance-IHHL_BPL') AS TOTAL_BPL ;

C = FOREACH B generate District,(float)TOTAL_BPL/BPL as performance , BPL;

D = FOREACH C generate District,ROUND_TO(performance*100,2) as percentage, BPL;

E = FILTER D BY percentage==100.0;

F = FOREACH E generate District;

STORE F INTO '/user/acadgild/dist';	
