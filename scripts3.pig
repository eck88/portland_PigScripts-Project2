---------Script per produrre un input a Weka: coppie <parola,occorrenza_normalizzata> per ogni specifica data------------------

%declare path1 's3://portland-bigdata/logs/';
input_file= load '$path1' using PigStorage('|','-tagsource') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray);

---Formattazione del testo in input
file_edit1= foreach input_file generate CONCAT((CONCAT((CONCAT(f1,f0)),f2)),f3) as parola;
file_edit2= foreach file_edit1 generate FLATTEN(STRSPLIT(parola,' ', 3));
file_edit3= foreach file_edit2 generate CONCAT(SUBSTRING($0,40,44),SUBSTRING($0,0,10)) as f0, $1 as f1, $2 as f2;

---Evito di matchare spazi bianchi e numeri, ma solo parole
tokenizeWords= foreach file_edit3 generate f0,FLATTEN(TOKENIZE(f2)) as f2;
tokenizeWords2 = FILTER tokenizeWords BY (f2 MATCHES '\\w+' AND NOT f2 MATCHES '\\d+' AND NOT f2 MATCHES '^\\w$') ;
tokenizeWords3= group tokenizeWords2 by (f0,f2);
tokenizeWords4= foreach tokenizeWords3 generate $0, COUNT($1);
tokenizeWords5= foreach tokenizeWords4 generate FLATTEN($0),$1;
tokenizeWords6= foreach tokenizeWords5 generate SUBSTRING($0,0,4) as f0, SUBSTRING($0,4,14) as f1, $1 as f2, $2 as f3;

--Cancello tutti i caratteri '-' dalle date
cleanRecords= foreach tokenizeWords6 generate $0 as f0, $2 as f1, REPLACE(f1,'-','') as f2 , $3 as f3;

---Ora dobbiamo NORMALIZZARE: join con input del log base
baseLog= load 'base_log' using PigStorage(',') as (f0:chararray,f1:chararray);
joinList= join cleanRecords by f1 LEFT OUTER, baseLog by $0;

--Filtro i record che occorrevano in baseLog da quelli che non vi occorrevano; sui primi, calcolo l'occorrenza normalizzata
filterNull= filter joinList by $5 is null;
filterNull2= foreach filterNull generate $0,$1,$2,$3;
filterNotNull= filter joinList by $5 is not null;
filterNotNull2= foreach filterNotNull generate $0,$1,$2,ABS((float)$3/(float)$5);

--Unisco i due insiemi di dati, creo una data puramente numerica e la normalizzo dividendola per 10000
final= union filterNull2,filterNotNull2;
final2= foreach final generate CONCAT((CONCAT($0,'_')),$1) as f0, $2 as f1, $3 as f2;
final3= foreach final2 generate f0, (CONCAT((CONCAT(SUBSTRING(f1,6,8),SUBSTRING(f1,4,6))),SUBSTRING(f1,2,4))) as f1, f2;
final4= foreach final3 generate f0, ((float)f1/10000) as f1 ,f2;

--Per far si che si generi un solo file di output
single_out= foreach (group final4 all) generate flatten(final4);
output_file= store single_out into 's3://portland-bigdata/output/results3/' using PigStorage(',');



