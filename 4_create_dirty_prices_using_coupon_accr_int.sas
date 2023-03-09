/**************************************************************************************************
* CREATE DIRTY PRICES IN ADDITION TO CLEAN PRICES
* STEP 1: Include FISD into daily price file and filter.
* STEP 2: Create prior cdate (from which interest accrues);
* Step 3: Accrue interest and create DIRTY PRICE;
*
**************************************************************************************************/

libname _scratch "/scratch/nwu/geo";		* Store created dataset here;
libname _home "/home/nwu/rickmann";



/**************************************************************************************************
*
* STEP 1: INCLUDE FISD INFO AND PLACE SOME FILTERS (e.g., coupon frequency)
*
**************************************************************************************************/

* Start w/ daily prices;
data trace_daily;
	set _home.prices_clean_daily;
run;

*==============================================================================;
* I. INCLUDE FISD MERGED ISSUANCE FILE - contains "new" coupon info;
*==============================================================================;

*1) Load the FISD file with the coupons;
%let fisd_vars = issue_id, complete_cusip as cusip9, dated_date, first_interest_date,
	interest_frequency, coupon,	coupon_change_indicator, day_count_basis, last_interest_date,
	maturity, offering_date, coupon_type, convertible, redeemable, bond_type, defaulted,
	principal_amt, action_type;
proc sql;
	create table coupon as
	select distinct &fisd_vars.
	from fisd.fisd_mergedissue
;quit;

*2) Load the coupon info into the trace data;
proc sql;
	create table trace_daily as
	select distinct l.*, r.*
	from trace_daily as l left join coupon as r
	on l.cusip9=r.cusip9
;quit;




*==============================================================================;
*II. IMPORT THE NEW_COUPON INFO FROM THE FISD COUPON_CHANGE FILE;
*==============================================================================;
* 1) Import last coupon change;
proc sql;
	create table trace_daily as
	select distinct l.*
					, r.new_coupon
					, r.change_date
	from trace_daily as l
	left join fisd.fisd_change_schedule (where=(not missing(issue_id))) as r
	on l.issue_id=r.issue_id
		and l.date > r.change_date
	group by l.issue_id, l.date
	having r.change_date=max(r.change_date)							/* but imported from latest date for a given issue_id*/
;quit;



*==============================================================================;
*III. APPLY SOME FILTERS.;
*==============================================================================;
data trace_daily;
	set trace_daily;
	if not missing(issue_id);
	if date >= offering_date;									/* after offering date */
	if date <= maturity;										/* before maturity */
	if date >= first_interest_date or interest_frequency="0"; 	/* first interest date took place */ /* WHY REQUIRE THAT FIRST_INTEREST DATE TOOK PLACE? */
	if interest_frequency in (0 1 2 4 12); 						/* coupon frequency */
run;
		















/**************************************************************************************************
*
* STEP 2: CREATE THE PRIOR_CDATE (most recent coupon date, from which interest accrues)
*
**************************************************************************************************/
		
*==============================================================================;
*I. CREATE THE PRIOR_CDATE (COUPON DATE) VARIABLE, FROM WHICH ACCRUED INTEREST ACCRUES;
* How? Use the first interst date, and then just add months to it;
*==============================================================================;
%macro date_maker;
	data trace_daily1;
		set trace_daily;

		* DEFINE MONTH AND DATE OF FIRST_INTEREST DATE. Do so because the coupon dates will be periodically after this date;
		mf = month(first_interest_date);	* mf = month of first interest date;
		df = day(first_interest_date);		* df = day of first interest date;

	*---------------------------------;
	* 1) INTEREST_FREQ = 0 - NO COUPON;
		if interest_frequency="0" then do;
			first_interest_date=offering_date;
			prior_cdate=first_interest_date;				/* In original code, I used DATED_DATE = Date from which interest accrues or from which original issue discount is amortized. */
			coupon=0;
		end;


	*-------------------------------------;
	* 2) INTEREST_FREQ = 1 - ANNUAL COUPON;
		if interest_frequency="1" then do;
			if date >= mdy(mf,df,year(date)) then  prior_cdate = mdy(mf,df,year(date));	* either that day+month from this year;
			if date < mdy(mf,df,year(date))  then prior_cdate = mdy(mf,df,year(date)-1); * or that day+month from last year;				/*Adjustment for February?*/
		end;



	*-------------------------------------;
	* 3) INTEREST FREQUENCY = 2 - SEMI-ANNUAL;
		if interest_frequency = "2" then do;

		* a) mf1 and mf2 are the two coupon months in any year.;
			if mf <= 6 then do;
				mf1=mf;
				mf2=mf+6;
			end;
			else if mf>6 then do;
				mf1=mf-6;
				mf2=mf;
			end;


		* b) df1 and df2 are the days in the two coupon months;
			%do i=1 %to 2;												/* WHAT IF df or mf ARE UNKNOWN? THEN I REDEFINE IT HERE. BUT MF WILL STILL BE UNKNOWN!*/
				df&i = df;
				if mf&i = 2				then df&i = min(28,df);
				if mf&i in (4 6 9 11)	then df&i = min(30,df);
			%end;


		*c) definte the coupon dates based on that, considering three contingencies;
			*i) if date > second cdate, then use the second;
			if mdy(mf2 , df2 , year(date)) <= date   then
				prior_cdate = mdy(mf2 , df2 , year(date));

			*ii) if date is between the two cdates, use the first;
			else if mdy(mf1, df1 , year(date))   <= date  <  mdy(mf2, df2, year(date))  then
				prior_cdate = mdy(mf1,df1,year(date));

			*iii) if date is smaller than first date, then use last years second;
			else if date < mdy(mf1, df1, year(date))  then
				prior_cdate = mdy(mf2,df2, year(date)-1);
		end;



	*---------------------------------;
	* 4) INTEREST FREQUENCY = 4;
		if interest_frequency = "4" then do;
			* a) BASED ON mf, DETERMINE THE 4 COUPON MONTHS;
			if mf in (1 4 7 10) then do;
				mf1=1;
				mf2=4;
				mf3=7;
				mf4=10;
			end;
			else if mf in (2 5 8 11) then do;
				mf1=2;
				mf2=5;
				mf3=8;
				mf4=11;
			end;
			else if mf in (3 6 9 12) then do;
				mf1=3;
				mf2=6;
				mf3=9;
				mf4=12;
			end;


			* b) DEFINE THE COUPON DAY FOR ONE OF THE 4;
			%do i=1 %to 4;
				df&i = df;
				if mf&i in (2) then df&i = min(28,df);
				if mf&i in (4 6 9 11) then df&i = min(30,df);
			%end;


			* c) DEFINE THE LAST COUPON DATES USING THE FOUR DATES. THERE ARE FIVE CONTINGENCIES;
			*i) DATE> 4th COUPON DATE;
			if mdy(mf4 , df4 , year(date)) <= date
				then prior_cdate = mdy(mf4 , df4 , year(date));

			*ii) DATE BETWEEN 4th and 3rd COUPON DATE;
			else if mdy(mf3 , df3 , year(date)) <= date < mdy(mf4 , df4 , year(date))
				then prior_cdate = mdy(mf3 , df3 , year(date));

			*iii) DATE BETWEEN 3rd and 2nd COUPON DATE, THEN 2nd IS THE LAST;
			else if mdy(mf2 , df2 , year(date)) <= date < mdy(mf3 , df3 , year(date))
				then prior_cdate = mdy(mf2 , df2 , year(date));

			*iv) DATE BETWEEN 2nd and 1st COUPON DATE, THEN 2nd IS THE LAST;
			else if mdy(mf1 , df1 , year(date)) <= date < mdy(mf2 , df2 , year(date))
				then prior_cdate = mdy(mf1 , df1 , year(date));

			*v) DATE SMALLER THAN LAST COUPON DATE;
			else if date < mdy(mf1 , df1 , year(date))
				then prior_cdate = mdy(mf4 , df4 , year(date)-1);
		end;




	*---------------------------------;
	* 5) INTEREST FREQUENCY = 12;
		if interest_frequency = "12" then do;


			* DEFINE THE LAST COUPON DATES USING THE FOUR DATES. THERE ARE FIVE CONTINGENCIES;
			* i) CASE1: df IS  SMALLER DAY THAN date, THEN prior_cdate IS THIS MONTH;
			if df <= day(date) then prior_cdate = mdy(month(date),df,year(date));

			* ii) CASE2: df IS LARGER DAY THAN date, THEN prior_cdate IS LAST MONTH;
			else if day(date) < df then do;

				**a) JAN: THEN last_cdate IN LAST YEAR;
				if month(date)=1 then prior_cdate = mdy(12,df,year(date)-1);

				**b) ELSE, SAME YEAR;
				else if month(date)>1 then do;
					*month;
					mf1 = month(date)-1;

					df1 = df;
					if month(date)-1=2 then df1 = min(df,28);
					if month(date)-1 in (4 6 9 11) then df1 = min(df,30);

					prior_cdate = mdy( mf1 , df1 , year(date) );
				end;
			end;
		end;



	*6) DROP AUXILIARY VARIABLES;
		drop mf mf1 mf2 mf3 mf4 df df1 df2 df3 df4;
		format prior_cdate yymmddn8.;
	run;
%mend;
%date_maker;








/**************************************************************************************************
*
* STEP 3: CREATE ACCRUED INTEREST AND DIRTY PRICE;
*	Dirty price = clean price + accrued interest.;
*
**************************************************************************************************/
		

*==============================================================================;
* I. CREATE ACCRUED INTEREST. IN DOING SO, INCORPORATE COUPON CHANGES;
* Last coupon date before or on the price date;
*==============================================================================;
* acc_int accrues between prior_cdate and this date.;

data trace_daily2;
	set trace_daily1;
	coupon_original = coupon;
	
	*----------------------------------------------------------;
	* I. WAY 1: USE ORIGINAL COUPON TO COMPUTE ACCRUED INTEREST;
	period_original = intck("day",prior_cdate,date)/360;
	acc_int_original = coupon_original * period_original;
	drop period_original;

	*----------------------------------------------------------;
	*II. WAY 2: USE NEW COUPON IF THERE WAS A COUPON CHANGE;
	* ... if there is a new coupon, use that one to accrue interest from the last coupon date.;
	* ... if not, use the original coupon ;
	if new_coupon^=coupon_original and not cmiss(new_coupon, change_date)
		then coupon_only_new = new_coupon;
	else coupon_only_new = coupon_original;
	period = intck("day",prior_cdate,date)/360;
	acc_int_only_new = coupon_only_new * period;
	drop period;

	
	*----------------------------------------------------------;
	*III. FOR ZERO-BONDS, ACCRUED INTEREST IS 0;
	if interest_frequency = "0" then do;
		acc_int_original = 0;
		acc_int_only_new = 0;
	end;

run;



*==============================================================================;
*II. DIRTY PRICE = clean price + Accrued Interest;
* Create prcd and dated (date that only exists when we have a clean price);
*==============================================================================;
data trace_daily2;
	set trace_daily2;
	*--------------------------------------;
	* 1. DEFINE DIRTY PRICE USING SOME COUPON ;
	acc_int = acc_int_original;				/* Decide which accrued interest do you want to use? Using old, newest, or subperiod one?*/
	prcd = prc + acc_int;					/* Dirty price */

	*--------------------------------------;
	* 2. Create DATED (only exists on dates with a dirty price) ;
	if not missing(prcd) then dated=date ;	/* DATED is the date from which the dirty price is */
	else dated=.;
	format dated YYMMDDN8.;

	*--------------------------------------;
	* DIRTY PRICES IN DIFFERENT WAYS;
	prcd_original = prc + acc_int_original;
	prcd_only_new = prc + acc_int_only_new;
run;

						/*NOTE: IF ACC_INT IS MISSING, I DO NOT SET IT TO ZERO AT THIS STAGE*/









/**************************************************************************************************
*
* SAVE FILE
*
**************************************************************************************************/
data _home.prices_dirty_daily;
	set trace_daily2;
run;


































	%macro where_to_dump;
						
							**************************************************************************************************;
							*IV. INCLUDE LAST AND NEXT CLEAN PRICE INTO THE TIMELINE
							**************************************************************************************************;
						
							*-------------------------------------------------;
							* Below, I create date_last and prc_last (and equivalently prcd_last,...). These are the price and date from the last date I have
							*  both prc and date. This way, the date corresponds to the price;
							*-------------------------------------------------;
						
							proc sort  data=timeline;
								by cusip9 date;
							run;
						
							data timeline;
								set timeline;
								by cusip9;
								retain lprc ldate lprcd ldated lprior_cdate lcoupon_only_new;
						
								*-----------------------------;
								* CASE1: FIRST OBSERVATION FOR A GIVEN FIRM;
								if first.cusip9 then do;
									** a) VAR_LAST missing;
									date_last=.;
									prc_last=.;
									dated_last=.;
									prcd_last=.;
									prior_cdate_last=.;
									coupon_only_new_last=.;
						
									** b) LVAR to carry forward: If var is there, save it. If not, carry forward missing value;
									** define ldate,lprc;
									if not cmiss(prc, date) then do;
										ldate=date;
										lprc=prc;
									end;
									else do;
										ldate=.;
										lprc=.;
									end;
						
									** same for the dirty vars. define ldated,lprcd;
									if not cmiss(prcd, dated) then do;
										ldated=dated;
										lprcd=prcd;
										lprior_cdate=prior_cdate;
										lcoupon_only_new=coupon_only_new;
									end;
									else do;
										ldated=.;
										lprcd=.;
										lprior_cdate=.;
										lcoupon_only_new=.;
									end;
								end;
						
						
						
						
								*-----------------------------;
								* CASE2: NOT FIRST OBSERVATION;
								else do;
									* a) VAR_LAST is last;
									date_last=ldate;
									prc_last=lprc;
									dated_last=ldated;
									prcd_last=lprcd;
									prior_cdate_last=lprior_cdate;
									coupon_only_new_last=lcoupon_only_new;
						
									* b) UPDATE ldate (carry_forward) if not missing;
									** prc, date;
									if not cmiss(prc, date) then do;
										ldate=date;
										lprc=prc;
									end;
									** prcd, dated: same;
									if not cmiss(prcd, dated) then do;
										ldated=dated;
										lprcd=prcd;
										lprior_cdate=prior_cdate;
										lcoupon_only_new=coupon_only_new;
									end;
								end;
						
								drop lprc ldate lprcd ldated lprior_cdate lcoupon_only_new;
								format date_last dated_last prior_cdate_last YYMMDDN8.;
							run;
						
									%m_count( "4. Include last trade-price in timeline.", timeline,qq, var1=cusip9, N1_name=cusip9);
						
						
						
						
						
						
						
						
						
						
							**************************************************************************************************;
							*VII. INCLUDE NEXT PRICE AND DIRTY PRICE
							**************************************************************************************************;
							proc sort  data=timeline; by cusip9 descending date;run;
						
							data timeline;
								set timeline;
								by cusip9;
								retain lprc ldate lprcd ldated ;
						
								*-----------------------------;
								* CASE1: FIRST OBSERVATION FOR A GIVEN FIRM;
								if first.cusip9 then do;
									** a) VAR_next missing;
									date_next=.;
									prc_next=.;
									dated_next=.;
									prcd_next=.;
						
									** b) LVAR to carry forward: If var is there, save it. If not, carry forward missing value;
									** define ldate,lprc;
									if not cmiss(prc, date) then do;
										ldate=date;
										lprc=prc;
									end;
									else do;
										ldate=.;
										lprc=.;
									end;
						
									** same for the dirty vars. define ldated,lprcd;
									if not cmiss(prcd, dated) then do;
										ldated=dated;
										lprcd=prcd;
						
									end;
									else do;
										ldated=.;
										lprcd=.;
						
									end;
								end;
						
								*-----------------------------;
								* CASE2: NOT FIRST OBSERVATION;
								else do;
									* a) VAR_next is next;
									date_next=ldate;
									prc_next=lprc;
									dated_next=ldated;
									prcd_next=lprcd;
						
						
									* b) UPDATE ldate (carry_forward) if not missing;
									** prc, date;
									if not cmiss(prc, date) then do;
										ldate=date;
										lprc=prc;
									end;
									** prcd, dated: same;
									if not cmiss(prcd, dated) then do;
										ldated=dated;
										lprcd=prcd;
									end;
								end;
						
								drop lprc ldate lprcd ldated  ;
								format date_next dated_next YYMMDDN8. ;
							run;
						
						
							* Bring into original order;
							proc sort  data=timeline;	by cusip9  date; run;
						
									%m_count( "5. Include next trade-price in timeline.", timeline,qq, var1=cusip9, N1_name=cusip9);
						
						
						
						
							**************************************************************************************************;
							*VI. MINI-CLEANING
							**************************************************************************************************;
							** Keep within first and last price obs;				/* OVERTHINK: WHY DO I USE THIS AGAIN? */
							data timeline;
								set timeline;
								if ( missing(prc) and missing(date_last) )
									or ( missing(prc) and missing(date_next) ) then delete;	/* Only keep dates within dates you have prices for. */
							run;
						
						
									%m_count( "** Keep only dates that are weakly between two price dates.", timeline,qq, var1=cusip9, N1_name=cusip9);
						
						
						
							** DELETE SOME FILES;
							proc datasets nolist; delete coupon fisd_change_schedule;run;quit;
						
						
							/* </editor-fold> PART 6: CREATE PRIOR COUPON DATE, ACCRUED INTEREST, AND DIRTY PRICE */
		

	%macro mend;



















/*     proc print data=trace_daily1 (obs=10);run;     */


