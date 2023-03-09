******************************************************************;
/* File 4 of 4: debt markets ... project */
/* Raw Returns, Asynchronous */

* Filename: 4_returns_asynch.sas ;
* Description: 
* Computing 3 types of returns w/clean & dirt prices ;

* Last modified: 12/23/2021 ;
******************************************************************;

******************************************************************;
/******************* Step 0: Libraries Defined *******************/

/* Location of dirty price dataset from 3rd file */
libname mylib "/~";					* Also save final data here ;
******************************************************************;

******************************************************************;
/**************** Step 1: Sort & Keep Last Price *****************/

/* Sort data by 9-digit CUSIP & execution date */
proc sort data = mylib.daily_dirty_price 
out = _prices_daily ;
by cusip9 date ;
run;

/* Generate last and next prices */
data prices_daily1;
set _prices_daily;
by cusip9;
retain lprc ldate lprcd ldated; 
if not first.cusip9 then do;
	prc_last = lprc;			* last price is retained variable ;
	prcd_last = lprcd;
	date_last = ldate;
end;
lprc = prc;
lprcd = prcd;
ldate = date;
drop lprc lprcd ldate;
format date_last YYMMDDN8.;
run;
	
******************************************************************;

******************************************************************;
/******************* Step 2: Compute Returns *********************/

/* Create final dataset */
data ret_async;
set prices_daily1;

* %-change in clean price ;
if prc_last > 0 then ret1 = (prc-prc_last)/prc_last;

* Change in clean scaled by last dirty price ;
if prcd_last>0 then ret2 = (prc-prc_last)/prcd_last;

* Change in clean + interest since last price / last dirty price;
if prcd_last>0 then do;
	days_since_last = date - date_last;
	* interest accrued since last price = interest since last coupon paid ;
	interest_since_last = coupon * days_since_last/360;		
	ret3 = (prc-prc_last + interest_since_last)/prcd_last; 
end;
run;

/* Save final dataset with all return types */
data mylib.ret_asynch;
set ret_async;
run;
******************************************************************;

******************************************************************;
/****************** Step 3: Summary Statistics *******************/

/* 1. How many days since last price */
* NOTE: there will be days for weekends/exchanges/...;
data test; 
set ret_async; 
if not missing(ret3); 
if 25 <= days_since_last < 50 then days_since_last=25;
if 50 <= days_since_last < 100 then days_since_last=50;
if 100 <= days_since_last < 150 then days_since_last=100;
if 150 <= days_since_last < 200 then days_since_last=150;
if days_since_last>200 then days_since_last=200;
run; 

title "Days since last price - Note 50 means 50 to 100, ...";
proc freq data=test; 
table days_since_last / nofreq ; 
run;  

/* 2. Distribution of returns (irrespective of how old last price is) */
title "Distribution of Returns (irrespective of how old last price is)";
proc means MEAN STD MIN P1 P5 Q1 MEDIAN Q3 P95 P99 MAX ;
	var ret1 ret2 ret3;
run;

/* 3. Distribution of only returns <= 7 days */
title "Distribution of Returns (last price at most 3 days old)";
data test; 
set ret_async; 
if date-date_last <= 7; 
run; 
proc means data=test 
MEAN STD MIN P1 P5 Q1 MEDIAN Q3 P95 P99 MAX ;
var ret1 ret2 ret3;
run;								

* 4. Distribution of only returns <= 7 days & after 2010 ;
title "Distribution of Returns (last price at most 3 days old)";
data test; 
set ret_async; 
if date-date_last <= 7; 
if year(date) >= 2010; 
run; 
proc means data = test 
MEAN STD MIN P1 P5 Q1 MEDIAN Q3 P95 P99 MAX ;
var ret1 ret2 ret3;
run;								
******************************************************************;
