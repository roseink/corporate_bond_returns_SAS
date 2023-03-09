******************************************************************;
/* File 3 of 4: debt markets ... project */
/* Get Accrued Interest & Dirt Price */

* Filename: 3_dirt_price.sas ;
* Description: 
* Compute dirty price & acrrued interest;

* Last modified: 12/23/2021 ;
******************************************************************;

******************************************************************;
/*************** PART 0: Load Libraries / Datasets ***************/
* Library location ;
libname mylib "~/"; 

* Get last daily end price file ;
data trace_daily_last;
set mylib.trace_daily_last;
run;

* Get value-weighted averaged daily price file ;
data trace_daily_vw;
set mylib.trace_daily_vw;
run;
******************************************************************;

******************************************************************;
/******************* PART 1: Attach Coupon Info ******************/

/* Joins: Merged Issue, Change Schedule and Default Info */

* Variables related to coupon info ;
%let fisd_vars = b.issue_id, b.dated_date, 
b.first_interest_date,b.interest_frequency, b.coupon, 
b.coupon_change_indicator, b.day_count_basis, 
b.last_interest_date, b.maturity, b.offering_date, 
b.coupon_type, b.convertible, b.redeemable, b.bond_type, 
b.defaulted, b.principal_amt, b.action_type;

* Merge current coupon info on last price data ;
proc sql;
	create table _merged1 as 
	select a.*, &fisd_vars. 
	from trace_daily_last as a 
	left join fisd.fisd_mergedissue
	(where=(not missing(issue_id))) as b 
	on a.cusip9 = b.complete_cusip;
quit;

* Repeat on value-weighted prices ;
proc sql;
	create table _merged2 as 
	select a.*, &fisd_vars.
	from trace_daily_vw as a 
	left join fisd.fisd_mergedissue
	(where=(not missing(issue_id))) as b 
	on a.cusip9 = b.complete_cusip;
quit;

* Merge information from coupon change schedule ;
proc sql;
	create table _trace_daily1 as 
	select a.*, b.change_date, b.new_coupon
	from _merged1 as a 
	left join fisd.fisd_change_schedule
	(where=(not missing(issue_id))) as b 
	on a.issue_id = b.issue_id
	and a.date > b.change_date
	group by a.issue_id, a.date
	having b.change_date = max(b.change_date);
quit;

* Same for value-weighted prices ;
proc sql;
	create table _trace_daily2 as 
	select a.*, b.change_date, b.new_coupon
	from _merged2 as a 
	left join fisd.fisd_change_schedule
	(where=(not missing(issue_id))) as b 
	on a.issue_id = b.issue_id 
	and a.date > b.change_date
	group by a.issue_id, a.date
	having b.change_date = max(b.change_date);
quit;

* Merge default information on last kept prices ;
proc sql;
	create table trace_daily1 as 
	select a.*, b.default_date, b.reinstated_date
	from _trace_daily1 as a 
	left join fisd.fisd_issue_default
	(where=(not missing(issue_id))) as b 
	on a.issue_id = b.issue_id;
quit;

* Merge default info on value-weighted prices ;
proc sql;
	create table trace_daily2 as 
	select a.*, b.default_date, b.reinstated_date
	from _trace_daily2 as a 
	left join fisd.fisd_issue_default
	(where=(not missing(issue_id))) as b 
	on a.issue_id = b.issue_id;
quit;

proc sql;
	create table checking as 
	select distinct day_count_basis from trace_daily1;
	quit;

******************************************************************;

******************************************************************;
/*************** PART 2: Most Recent Coupon Date *****************/

/* Get Preliminary Previous and Next Coupon Dates */
%macro coupon_dates(dsetin, dsetout);
data &dsetout.;
set &dsetin.;
* Get these 2 pieces of info ;
COUPMONTH = intck("month", first_interest_date, stlmnt_dt);
NCOUPS = input(interest_frequency, 8.);
* CASE 1: before first_interest_date ;
if stlmnt_dt <= first_interest_date then do;
	prevcoup = dated_date;
	nextcoup = first_interest_date;
end;
* CASE 2: otherwise compute as follows ;
if stlmnt_dt > first_interest_date then do;
	if interest_frequency ^= "0" then do;
		* If day is not EOM, then compute as ;
		if day(first_interest_date) ^= day(
			intnx('month', mdy(month(first_interest_date), 
						1, year(first_interest_date)), 0, 'e'))
		then do;
			prevcoup = intnx("month", first_interest_date,
				((INT(COUPMONTH * NCOUPS/12)) * 12/NCOUPS), "sameday");
			nextcoup = intnx("month",first_interest_date,
				(1 + INT(COUPMONTH * NCOUPS/12)) * 12/NCOUPS,"sameday");
		end;
		* If day is last day of month, then change alignment ;
		if day(first_interest_date) = day(
			intnx('month', mdy(month(first_interest_date), 
						1, year(first_interest_date)), 0, 'e'))
		then do;
			prevcoup = intnx("month", first_interest_date,
				((INT(COUPMONTH * NCOUPS/12)) * 12/NCOUPS), "end");
			nextcoup = intnx("month",first_interest_date,
				(1 + INT(COUPMONTH * NCOUPS/12)) * 12/NCOUPS, "end");	
		end;
	end;
	if interest_frequency = "0" then prevcoup = dated_date;
end; 

* CHECK THAT ALL SETTLEMENT DATES ARE IN THE CORRECT RANGE ;
if stlmnt_dt < prevcoup and interest_frequency ^= "0" then do;
	* If day is not EOM, then compute as ;
	if day(first_interest_date) ^= day(
			intnx('month', mdy(month(first_interest_date), 
						1, year(first_interest_date)), 0, 'e'))
	then do;
		prevcoup = intnx("month", first_interest_date,
			(-1 + INT(COUPMONTH * NCOUPS/12)) * 12/NCOUPS,"sameday");
		nextcoup = intnx("month", first_interest_date,
			(INT(COUPMONTH * NCOUPS/12)) * 12/NCOUPS,"sameday");
	end;
	* If day is last day of month, then change alignment ;
	if day(first_interest_date) = day(
		intnx('month', mdy(month(first_interest_date), 
					1, year(first_interest_date)), 0, 'e'))
	then do;
		prevcoup = intnx("month", first_interest_date,
			(-1 + INT(COUPMONTH * NCOUPS/12)) * 12/NCOUPS, "end");
		nextcoup = intnx("month", first_interest_date,
			(INT(COUPMONTH * NCOUPS/12)) * 12/NCOUPS, "end");	
	end;
end;

%mend;

* Run macro (unadjusted) ;
%coupon_dates(trace_daily1, _temp);

/* Adjust Coupon Dates for holiday & "business day follow" convention */
%macro biz_day_holiday_adj(dsetin, dsetout);
data _adj_dates;
set &dsetin.;
* Adjust PREVCOUP ;
if stlmnt_dt > first_interest_date then do;
if weekday(prevcoup) = 1 then do;					* if Sunday ;
	next_day = intnx('day', prevcoup, 1);			* roll over to Monday ;
	* But if Monday in next month -> Friday ;
	if month(next_day) = month(prevcoup) + 1 			
		then prevcoup = intnx('day', prevcoup, -2);		
	else prevcoup = next_day;
end;
if weekday(prevcoup) = 7 then do;					* if Saturday ;
	next_day = intnx('day', prevcoup, 2);			* roll over to Monday ;
	* If Monday in next month -> Friday ;
	if month(next_day) = month(prevcoup) + 1 			
		then prevcoup = intnx('day', prevcoup, -1);		
	else prevcoup = next_day;
end;
end;

* Repeat adjustment on NEXTCOUP ;
if weekday(nextcoup) = 1 then do;					* if Sunday ;
	next_day2 = intnx('day', nextcoup, 1);			* roll over to Monday ;
	* But if Monday in next month -> Friday ;
	if month(next_day2) = month(nextcoup) + 1 			
		then nextcoup = intnx('day', nextcoup, -2);		
	else nextcoup = next_day2;
end;
if weekday(nextcoup) = 7 then do;					* if Saturday ;
	next_day2 = intnx('day', nextcoup, 2);			* roll over to Monday ;
	* If Monday in next month -> Friday ;
	if month(next_day2) = month(nextcoup) + 1 			
		then nextcoup = intnx('day', nextcoup, -1);		
	else nextcoup = next_day2;
end;

format prevcoup nextcoup next_day date9.;
drop ncoups coupmonth ;
run;

/* PREVCOUP and NEXTCOUP holiday adjustment */
proc sql;
	create table _holidays1 as 
	select a.*, b.name as holiday_name
	from _adj_dates as a left join mylib.sifma as b 
	on a.prevcoup = b.date 
	and b.closing_time = "00:00:00"t 
	and a.stlmnt_dt > a.first_interest_date;
quit;
proc sql;
	create table _holidays2 as 
	select a.*, b.name as holiday_name2
	from _holidays1 as a left join mylib.sifma as b
	on a.nextcoup = b.date
	and b.closing_time = "00:00:00"t;
quit;

/* Get the next weekday, if in next month go back to previous trade day */
data _holiday_adj;
set _holidays2;
if not missing(holiday_name) then do;
	if 1 <= weekday(prevcoup) <= 5 then 
		adj_dt = intnx('day', prevcoup, 1);
	if weekday(prevcoup) = 6 then 
		adj_dt = intnx('day', prevcoup, 3);
	if weekday(prevcoup) = 7 then 
		adj_dt = intnx('day', prevcoup, 2);
	if month(adj_dt) > month(prevcoup) then do ;
		new_adj_dt = intnx('day', prevcoup, -1);
		if weekday(new_adj_dt) = 7 then new_prevcoup = intnx('day', adj_dt, -1);
		if weekday(new_adj_dt) = 1 then new_prevcoup = intnx('day', adj_dt, -2);
		new_prevcoup = new_adj_dt;
	end;
	else new_prevcoup = adj_dt;
end;
if not missing(holiday_name2) then do;					* repeat on nextcoup ;
	if 1 <= weekday(nextcoup) <= 5 then 
		adj_dt2 = intnx('day', nextcoup, 1);
	if weekday(nextcoup) = 6 then 
		adj_dt2 = intnx('day', nextcoup, 3);
	if weekday(nextcoup) = 7 then 
		adj_dt2 = intnx('day', nextcoup, 2);
	if month(adj_dt2) > month(nextcoup) then do ;
		new_adj_dt2 = intnx('day', nextcoup, -1);
		if weekday(new_adj_dt2) = 7 then new_nextcoup = intnx('day', adj_dt2, -1);
		if weekday(new_adj_dt2) = 1 then new_nextcoup = intnx('day', adj_dt2, -2);
		new_nextcoup = new_adj_dt2;
	end;
	else new_nextcoup = adj_dt2;
end;
format adj_dt new_prevcoup new_nextcoup new_adj_dt
new_adj_dt2 adj_dt2 DATE9.;
run;

* Drop extra columns & set dates equal to adjusted, if necessary ;
data &dsetout.;
set _holiday_adj;
if not missing(new_prevcoup) then prevcoup = new_prevcoup;
if not missing(new_nextcoup) and not missing(nextcoup) 
	then nextcoup = new_nextcoup;
drop next_day next_day2 holiday_name holiday_name2 adj_dt adj_dt2
new_adj_dt new_adj_dt2 new_prevcoup new_nextcoup;
run;

%mend;

/* Run macro here */
%biz_day_holiday_adj(_temp, _trace_coupinfo);

/* Run verification.sas at this point to check valid dates */
* Test this dataset generated by above macro ;
data mylib._trace_coupinfo;
set _trace_coupinfo;
run;

* Test using this dataset on the previous macro to compare ;
data mylib.trace_daily1;
set trace_daily1;
run;

******************************************************************;

******************************************************************;
/************** PART 4: Accrued Interest & Dirty Price ***********/

/* DATDIF computes period accurately */
data _acc_int; 
set _trace_coupinfo;
coupon_use = coupon;

* Handle coupon change here ;
if new_coupon ^= coupon and not cmiss(new_coupon, change_date)
	then coupon_use = new_coupon;
* Handle zero bonds here ;
if (interest_frequency = "0") then coupon_use = 0;
	
if day_count_basis = "30/360" then 
	prev_stl = datdif(prevcoup, stlmnt_dt, '30/360');
	if not missing(nextcoup) then 
		next_prev = datdif(prevcoup, nextcoup, '30/360');
if day_count_basis = "ACT/360" then 
	prev_stl = datdif(prevcoup, stlmnt_dt, 'ACT/360');
	if not missing(nextcoup) then 
		next_prev = datdif(prevcoup, nextcoup, 'ACT/360');
	
* Handle defaulted bonds here ;
if (defaulted = "Y") and (stlmnt_dt > default_date >= prevcoup) then do;
	if day_count_basis = "30/360" then 
		prev_stl = datdif(prevcoup, default_date, '30/360');
	if day_count_basis = "ACT/360" then 
		prev_stl = datdif(prevcoup, default_date, 'ACT/360');
end;

* Compute AI as follows ;
freq = input(interest_frequency, best8.);
if freq = 0 then acc_int = 0;						* zero bonds ;
else acc_int = coupon_use/freq * (prev_stl/next_prev);

* Handle ACT/360 real effective annual interest rate here ;
if day_count_basis = "ACT/360" then 
	acc_int = coupon_use/freq * 365/360 * prev_stl/next_prev;
	
drop freq;
run;

/* My version of dirty price */
data prc_final;
set _acc_int;
prcd = prc + acc_int;
*where not missing(acc_int);
if not missing(prcd) then dated = stlmnt_dt;		* dirt price date ;
else dated = . ;
format dated YYMMDDN8.;
run;

/* Missing accrued interest bonds all have no coupon information! */
proc sql;
	create table issueids_nocoup as 
	select distinct issue_id
	from _acc_int(where=(missing(acc_int)));
quit;

data check_dated_date;
set _acc_int;
where weekday(dated_date) in (1,7);
run;
proc sql;

    create table coupon as

    select distinct *, (coupon>0 and not missing(coupon)) as d_nonzero_coupon

    from _trace_coupinfo

    having interest_frequency="0"

;quit;

******************************************************************;

******************************************************************;
/************* PART 5: Clean & Save Datasets Generated ***********/

/* Use same library as before */
data mylib.daily_dirty_price;
set prc_final;
run;
******************************************************************;