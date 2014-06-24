----------Script per vedere, per ogni giornata e per ogni macchina, le parole più ricorrenti (sempre per macchina)------------

%declare path1 's3://portland-bigdata/logs/';
input_file= load '$path1' using PigStorage('|','-tagsource') as (f0:chararray,f1:chararray,f2:chararray,f3:chararray);

---Formattazione del testo in input
file_edit1= foreach input_file generate CONCAT((CONCAT((CONCAT(f1,f0)),f2)),f3) as parola;
file_edit2= foreach file_edit1 generate FLATTEN(STRSPLIT(parola,' ', 3));
file_edit3= foreach file_edit2 generate CONCAT(SUBSTRING($0,0,10),SUBSTRING($0,40,44)) as f0, $1 as f1, $2 as f2;

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
cleanRecord= foreach tokenizeWords7 generate SUBSTRING($0,0,10) as f0, SUBSTRING($0,10,14) as f1, $1 as f2;
cleanRecord2= order cleanRecord by f0;
cleanRecord3= foreach cleanRecord2 {
	topList= TOP(6,1,f2);
	generate f0,f1,topList;
}
cleanRecord4= foreach cleanRecord3{
	orderTop= order $2 by $1 DESC;
	generate f0,f1,orderTop; 
}

--Qui una serie di trasformazioni per rendere il file ordinato e leggibile come .csv (al fine di creare dei grafici Excel)
forCsv_1= foreach cleanRecord4 generate f0,f1, FLATTEN(f2); 

--Per far si che si generi un solo file di output
single_out= foreach (group forCsv_1 all) generate flatten(forCsv_1);
output_file= store single_out into 's3://portland-bigdata/output/results4/' using PigStorage(',');



