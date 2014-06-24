------Script per vedere, ogni giorno,in ogni intervallo da ora in ora, le parole più ricorrenti (sempre per macchina)---------

%declare path1 's3://portland-bigdata/logs/';
input_file= load '$path1' using PigStorage('|','-tagsource') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray);

---Formattazione del testo in input
file_edit1= foreach input_file generate CONCAT((CONCAT((CONCAT(f1,f0)),f2)),f3) as parola;
file_edit2= foreach file_edit1 generate FLATTEN(STRSPLIT(parola,' ', 3));
file_edit3= foreach file_edit2 generate CONCAT(SUBSTRING($0,0,13),SUBSTRING($0,40,44)) as f0, $1 as f1, $2 as f2;

---Evito di matchare spazi bianchi e numeri, ma solo parole
tokenizeWords= foreach file_edit3 generate f0,FLATTEN(TOKENIZE(f2)) as f2;
tokenizeWords2 = FILTER tokenizeWords BY (f2 MATCHES '\\w+' AND NOT f2 MATCHES '\\d+' AND NOT f2 MATCHES '^\\w$') ;
tokenizeWords3= group tokenizeWords2 by (f0,f2);
tokenizeWords4= foreach tokenizeWords3 generate $0, COUNT($1);
tokenizeWords5= foreach tokenizeWords4 generate FLATTEN($0),$1;
tokenizeWords6= group tokenizeWords5 by $0;


--Ora voglio che ogni data-ora-macchina abbia la lista delle parole con il numero delle occorrenze a fianco
tokenizeWords7= foreach tokenizeWords6 {
	wordsList= foreach $1 generate $1 as f1, $2 as f2;
	wordsList2= order wordsList by f2 DESC;
	generate $0 as f0, wordsList2 as f1;
}

--Pulizia dei record, prendo solo le 6 parole più ricorrenti in ogni ora della specifica data
cleanRecord= foreach tokenizeWords7 generate SUBSTRING($0,0,10) as f0, SUBSTRING($0,11,13) as f1, 
	      SUBSTRING($0,13,17) as f2, $1 as f3;
cleanRecord2= order cleanRecord by f2;
cleanRecord3= foreach cleanRecord2 {
	topList= TOP(6,1,f3);
	generate f0,f1,f2,topList;
}
cleanRecord4= foreach cleanRecord3{
	orderTop= order $3 by $1 DESC;
	generate f0,f1,f2,orderTop; 
}

--Qui una serie di trasformazioni per rendere il file ordinato e leggibile come .csv (al fine di creare dei grafici Excel)
forCsv_1= foreach cleanRecord4 generate CONCAT((CONCAT($0,$2)),$1) as f0, $3 as f1;
forCsv_2= order forCsv_1 by f0;
forCsv_3= foreach forCsv_2 generate SUBSTRING($0,0,10) as f0, SUBSTRING($0,10,14) as f1 , SUBSTRING($0,14,16) as f2 ,$1 as f3; 
forCsv_4= foreach forCsv_3 generate f0,f1,f2, FLATTEN(f3); 

--Per far si che si generi un solo file di output
single_out= foreach (group forCsv_4 all) generate flatten(forCsv_4);
output_file= store single_out into 's3://portland-bigdata/output/results2' using PigStorage(',');



