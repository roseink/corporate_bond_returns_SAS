*******************************************************************;
/* File 2 of 4: debt markets ... project */
/* Create daily clean price */

* Filename: 2_daily_end_price.sas ;
* Description: set the daily ending price based on 
* 1) Value-weighted average of intraday prices, or 
* 2) Latest intraday price ;

* Last modified: 12/10/2021 ;
******************************************************************;

******************************************************************;
/************************* Workspace clean ***********************/

* Run once at start ;
proc datasets kill noprint; run;
******************************************************************;

******************************************************************;
/*********************** Define libraries ************************/

* Location of clean post TRACE enhanced & FISD data ;
libname mylib "~/"; 
******************************************************************;

******************************************************************;
/************* Step 0: Load Data & Rename Variables **************/

* Load dataset and rename necessary columns ;
data _clean;
set mylib.clean_post_en_fisd;
rename cusip_id = cusip9 
trd_exctn_dt = date
rptd_pr = prc
entrd_vol_qt = volume;
run;

******************************************************************;

******************************************************************;
/*********** Step 1: Keep Open Market/Weekday Trades *************/

* Market close at 5pm EST ;
data _clean;
set _clean;
if trd_exctn_tm <= '17:00:00't;					
if 2 <= weekday(date) <= 6;	
run;			
******************************************************************;

******************************************************************;
/************** Step 2: Set End Daily Price (2 ways) *************/

* Macro: sets daily end price ;
%macro daily_prc(dsetin, dsetout, option) ;
proc sort data = &dsetin. out = _sort1;		* Sort first ;
	by cusip9 date trd_exctn_tm;
	run;

%if &option. = 1 %then %do;				* Value-weighted ;
	proc means noprint data=_sort1;
	by cusip9 date;
	weight volume;
	output out = &dsetout. mean(prc)=prc;
	run;
	
	* Get rid of auxiliary columns generated above ;
	data &dsetout.;
	set &dsetout.;
	drop _freq_ _type_;
	run;
%end ;

%if &option. = 2 %then %do;				* Last intraday ;
	data &dsetout.; 
	set _sort1; 
	by cusip9 date trd_exctn_tm;
	if last.date;
	run;
%end;
%mend;

* Call twice ;
%daily_prc(_clean, _daily_prc_vw, 1);
%daily_prc(_clean, _daily_prc_last, 2);

******************************************************************;

******************************************************************;
/********************* Step 2: Save Datasets *********************/

* Save value-weighted average daily price ;
data mylib.trace_daily_vw;
set _daily_prc_vw;
run;

* Save last intraday price ;
data mylib.trace_daily_last;
set _daily_prc_last;
run;

******************************************************************;

******************************************************************;
/********************* Step 3: Summary Stats *********************/

* Rename for convenience/code-reuse ;
data trace_daily1;
set _daily_prc_last;
run;

* Make column of days last ;
proc expand data = _daily_prc_last out = trace_daily2 
method=none;
by cusip9;
convert date = date_lag / transformout = (lag 1);
run;
data trace_daily2;
set trace_daily2;
days_last = date-date_lag;
run;

* PLOT WEEKDAY OF OBS;	
data test; 
set trace_daily1; 
day=weekday(date); 
run;
proc freq data=test; 
tables day; 
run;

* PLOT TIME OF THE DAY OF TRADES;
* Test: Plot the time of the day;
data test; 
set trace_daily1; 
hour=hour(trd_exctn_tm); 
run;
proc sql; 
	create table a as 
	select distinct hour, count(*) as N
	from test 
	group by hour;
run;
proc print data=a; run;

* PLOT DAYS SINCE LAST OBS IN FINAL FILE;	
proc sql; 
	create table a as 
	select distinct days_last, count(*) as N
	from trace_daily2
	group by days_last;
run;
proc freq data=trace_daily2; table days_last; run;
proc print data=a; run;

******************************************************************;