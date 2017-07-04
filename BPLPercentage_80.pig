REGISTER /usr/local/pig/lib/piggybank.jar;
DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath();


A =  LOAD '/flume_import/StatewiseDistrictwisePhysicalProgress.xml' using org.apache.pig.piggybank.storage.XMLLoader('row') as (x:chararray);

B = FOREACH A GENERATE XPath(x, 'row/District_Name') AS District, (long)XPath(x, 'row/Project_Objectives_IHHL_BPL') AS BPL,
(long)XPath(x, 'row/Project_Performance-IHHL_BPL') AS TOTAL_BPL ;

REGISTER /home/acadgild/pigjars/pig_udf.jar;

DEFINE filerfunc custom.func.NewFilter;

C= FILTER B BY filerfunc(TOTAL_BPL,BPL);

D = FOREACH C generate District;

STORE D INTO '/user/acadgild/district';


