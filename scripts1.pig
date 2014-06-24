----------Script per vedere in ogni giorno, su ogni macchina, come aumenta la verbosità dei log di ora in ora.----------------

%declare path1 's3://portland-bigdata/logs/';
input_file= load '$path1' using PigStorage('|','-tagsource') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray);

---Formattazione del testo in input
file_edit1= foreach input_file generate CONCAT((CONCAT((CONCAT(f1,f0)),f2)),f3) as parola;
file_edit2= foreach file_edit1 generate FLATTEN(STRSPLIT(parola,' ', 3));
file_edit3= foreach file_edit2 generate CONCAT(SUBSTRING($0,0,13),SUBSTRING($0,40,44)) as f0, $1 as f1, $2 as f2;

---Raggruppo e ordino per ora e per data, conto il numero dei record presenti ad ogni ora
groupDateHourVm_1= group file_edit3 by f0;
groupDateHourVm_2= foreach groupDateHourVm_1 generate $0 as f0, SIZE($1) as f1;

orderGroup_1= group groupDateHourVm_2 by (SUBSTRING(f0,0,10),SUBSTRING(f0,13,17));
orderGroup_2= foreach orderGroup_1 {
		clean= foreach $1 generate SUBSTRING($0,11,13), $1;
		generate $0,clean;}
orderGroup_3= foreach orderGroup_2{
		clean2= order $1 by $0;
		generate $0,clean2;} 

--Qui una serie di trasformazioni per rendere il file ordinato e leggibile come .csv (al fine di creare dei grafici Excel)
forCsv_1= foreach orderGroup_3 generate FLATTEN($0),$1;
forCsv_2= foreach forCsv_1 generate CONCAT($0,$1), FLATTEN($2);
forCsv_3= foreach forCsv_2 generate CONCAT($0,$1) as f1, $2 as f2;
forCsv_4= order forCsv_3 by f1;
forCsv_5= foreach forCsv_4 generate SUBSTRING($0,0,10) as f0, SUBSTRING($0,10,14) as f1 , SUBSTRING($0,14,16) as f2, $1 as f3;

--Per far si che si generi un solo file di output
single_out= foreach (group forCsv_5 all) generate flatten(forCsv_5);
output_file= store single_out into 's3://portland-bigdata/output/results1' using PigStorage(',');

