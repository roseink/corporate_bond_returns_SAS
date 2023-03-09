******************************************************************;
/* File 1 of 4: debt markets ... project */
/* TRACE data processing, Nielsen & Asquith Filters */

* Filename: 1_nielsen_asquith.sas ;
* Procedure for TRACE data pre-processing ;
* Based on Dick-Nielsen (2014) and Asquith filters ;
* Additional filters are from recent literature:  
* Guo et al. (2020), He et al. (2021), & Bai et al.
* (2019) ; 
* NOTE: final dataset only uses post-TRACE Enhanced 
* data so far ;

* Last modified: 12/10/2021 ;
******************************************************************;

******************************************************************;
/************************* Workspace clean ***********************/

* Run once at start ;
proc datasets kill noprint; run;
******************************************************************;

******************************************************************;
/********************** Define libraries *************************/

* For convenience ;
libname fisd ('/wrds/mergent/sasdata/fisd' 
'/wrds/mergent/sasdata/naic');
libname trace  '/wrds/trace/sasdata/standard';
libname tracee '/wrds/trace/sasdata/enhanced';
******************************************************************;

******************************************************************;
/************************ Data downloads *************************/

* Query and download trace enhanced file ;
data trace_enhanced; 
set tracee.trace_enhanced; 
where cusip_id ne '' ; 
run;

* Separate post-2/6/2012 sample ;
data post;
set trace_enhanced;
where trd_rpt_dt >= '06Feb2012'd;
run;

* Separate pre-2/6/2012 bond sample ;
data pre;
set trace_enhanced;
where trd_rpt_dt < '06Feb2012'd;
run;
******************************************************************;

******************************************************************;
/****************** Part 1: Post-2/6/2012 Sample *****************/

/* Data filtering */

* Filter out T and R trade status ;
data post_TR;
set post;
where trc_st in ('T', 'R');
run;

* Filter out cancels and corrections trade status ;
data post_XC; 
set post; 
where trc_st in ('X', 'C'); 
run; 

* Filter out reversals ;
data post_Y;  
set post; 
where trc_st in ('Y'); 
run; 

/* Delete cancellations and corrections */

* Match via 7 keys and message sequence number ;
proc sql;
	create table _clean_post1 as 
	select distinct a.*, b.trc_st as trc_st_xc
	from post_TR as a left join post_XC as b
	on a.msg_seq_nb = b.msg_seq_nb
	and a.cusip_id = b.cusip_id
	and a.bond_sym_id = b.bond_sym_id
	and a.trd_exctn_dt = b.trd_exctn_dt
	and a.trd_exctn_tm = b.trd_exctn_tm
	and a.entrd_vol_qt = b.entrd_vol_qt
	and a.rptd_pr = b.rptd_pr
	and a.rpt_side_cd = b.rpt_side_cd
	and a.cntra_mp_id = b.cntra_mp_id;
quit;

* Delete rows that are matched ;
data _clean_post1;
set _clean_post1;
where trc_st_xc = "";
drop trc_st_xc;
run;

/* Delete reversal records */

* Match reversals ;
proc sql;
	create table _clean_post2 as
	select * from _clean_post1 as a,
	/* the below set has obs that match to reversal report*/
	( (select cusip_id, entrd_vol_qt, rptd_pr, trd_exctn_dt, 
	trd_exctn_tm, rpt_side_cd, cntra_mp_id, 
	msg_seq_nb from _clean_post1)
	  except (select cusip_id, entrd_vol_qt, rptd_pr, 
	  trd_exctn_dt, trd_exctn_tm, rpt_side_cd, cntra_mp_id, 
	  orig_msg_seq_nb from post_Y)
	) as b
	/* keep original obs not corresponding to a reversal obs */
	where a.cusip_id = b.cusip_id
	and a.entrd_vol_qt = b.entrd_vol_qt
	and a.rptd_pr = b.rptd_pr
	and a.trd_exctn_dt = b.trd_exctn_dt
	and a.trd_exctn_tm = b.trd_exctn_tm
	and a.rpt_side_cd = b.rpt_side_cd
	and a.cntra_mp_id = b.cntra_mp_id
	and a.msg_seq_nb = b.msg_seq_nb;
quit;	

******************************************************************;

******************************************************************;
/************* Part 2: Remove Double-Counted Records *************/

/* Sort out Agency Transactions */

* Identify and drop agency transactions that are non-commission trades ;
data _clean_ag1;
set _clean_post2;
if rpt_side_cd = "B" then agency = buy_cpcty_cd;
if rpt_side_cd = "S" then agency = sell_cpcty_cd;
if agency = "A" and cntra_mp_id = "C" and cmsn_trd = "N" then delete;
run;

* Delete buy-side and make flag of double-report interdealer trades ;
data _clean_ag2 (drop=agency);
set _clean_ag1;
if rpt_side_cd = "B" and cntra_mp_id = "D" then delete;
if rpt_side_cd = "S" and cntra_mp_id = "D" then rpt_side_cd = "D";
run;

******************************************************************;

******************************************************************;
/********** Part 3: Additional Processing **********/

/* More Deletions Made Here */

* Before issuance, give up, secondary market, etc. ;
data _clean_trace ;
set _clean_ag2;
if wis_fl = 'N';  						* delete when-issued trades ;
if trdg_mkt_cd in ('S2', 'P1', 'P2') then delete; 	* no secondary mkt trades ;
if spcl_trd_fl = 'Y' then delete;		* no special trade conditions ;
if days_to_sttl_ct < 2;					* no days to settlement >= 6 ;
if sale_cndtn_cd = 'C' then delete;		* no non-cash sales ;
if cmsn_trd = 'N';						* no commissioned trades;
if agu_qsr_id in ('A','Q') then delete; * no automatic give-ups;
if sub_prdct ne "CORP" then delete;		* no other equity-linked bonds ;
run;

******************************************************************;

******************************************************************;
/****************** Part 4: Merge with FISD Data *****************/

/* Step 1: Merge Characteristics w/Redemption info */

* Merge on FISD's common identifier: ISSUE ID ;
proc sql;
	create table fisd_mergedissue_redemp as 
	select a.*, b.callable, b.sinking_fund
	from fisd.fisd_mergedissue as a 
	left join fisd.fisd_redemption as b
	on a.issue_id = b.issue_id;
quit;

* Download column names ;
%let vars_import = b.bond_type, b.offering_date, 
b.maturity, b.foreign_currency, b.principal_amt, b.offering_amt, 
b.convertible, b.interest_frequency, b.coupon_type, b.rule_144a, 
b.exchangeable, b.putable, b.coupon_change_indicator, 
b.treasury_maturity, b.delivery_date, b.issue_id, b.callable, 
b.sinking_fund, b.issuer_id, b.issuer_cusip;

/* Step 2: Merge combined dataset with TRACE Enhanced Post */

* Match on 9-digit cusip ;
proc sql;
	create table _trace_1 as
	select distinct a.*, &vars_import.
	from _clean_trace as a
	left join fisd_mergedissue_redemp
	(where=( not missing(complete_cusip) )) as b
	on a.cusip_id = b.complete_cusip
	having not missing(b.issue_id) ;
quit;

******************************************************************;


******************************************************************;
/************ Part 5: Asquith Filter on Merged Data **************/

/* Step 1: Cut Convertible, Exchangeable, & 144A bonds */
data _trace_merged;
set _trace_1;
if convertible = "Y" then delete;
if exchangeable = "Y" then delete;
if rule_144A = "Y" then delete;
run;

/* Step 2: Deal with Timing Issues */
data _trace_merged2;
set _trace_merged;
if trd_exctn_dt <= offering_date then delete; 	* execute b4 date ;
if trd_exctn_dt >= maturity then delete;		* execute after maturity ;
if trd_exctn_dt = trd_rpt_dt;					* same report & execute date ;
run;
 
/* Step 3: Issue Size & Volume Problems */
data _trace_merged3;
set _trace_merged2;
if offering_amt < 100 then delete; 				* units: 1000 ;
if missing(offering_amt) then delete;			* no missing issues ;
if (entrd_vol_qt / 1000)/offering_amt < 0.5 ;	* asquith volume/issue ratio ;
if entrd_vol_qt >= 100000;						* volume is in raw units ;
run;

/* Step 4: delete SIFMA Holiday Trades */

* Create SIFMA holiday schedule dataset ;
data work.sifma;
	infile datalines dlm=',' dsd;
	length name $12 closing_time $10 date $10; 
    input name $ closing_time $ date $;
    datalines;
    NYD, 00:00:00, 01/01/2021
    NYE, 14:00:00, 12/31/2020
    CH, 00:00:00, 12/25/2020	
    CE, 14:00:00, 12/24/2020
    BF, 14:00:00, 11/27/2020
    TH, 00:00:00, 11/26/2020
    VD, 00:00:00, 11/11/2020
    CD, 00:00:00, 10/12/2020
    LD, 00:00:00, 09/07/2020
    J4, 00:00:00, 07/03/2020
    J4E, 14:00:00, 07/02/2020
    MD, 00:00:00, 05/25/2020
    MDWknd, 14:00:00, 05/22/2020
    GF, 00:00:00, 04/10/2020
    ES, 14:00:00, 04/09/2020
    PD, 00:00:00, 02/17/2020
    MLK, 00:00:00, 01/20/2020
    NYD, 00:00:00, 01/01/2020
    NYE, 14:00:00, 12/31/2019
    CH, 00:00:00, 12/25/2019
    CE, 14:00:00, 12/24/2019
    BF, 14:00:00, 11/29/2019
    TH, 00:00:00, 11/28/2019
    VD, 00:00:00, 11/11/2019
    CD, 00:00:00, 10/14/2019
    LD, 00:00:00, 09/02/2019
    J4, 00:00:00, 07/04/2019
    J4E, 14:00:00, 07/03/2019
    MD, 00:00:00, 05/27/2019
    MDWknd, 14:00:00, 05/24/2019
    GF, 00:00:00, 04/19/2019
    ES, 14:00:00, 04/18/2019
    PD, 00:00:00, 02/18/2019
    MLK, 00:00:00, 01/21/2019
    NYD, 00:00:00, 01/01/2019
    NYE, 14:00:00, 12/31/2018
    CH, 00:00:00, 12/25/2018
    CE, 14:00:00, 12/24/2018
    BF, 14:00:00, 11/23/2018
    TH, 00:00:00, 11/22/2018
    VD, 00:00:00, 11/12/2018
    CD, 00:00:00, 10/08/2018
    LD, 00:00:00, 09/03/2018
    J4, 00:00:00, 07/04/2018
    J4E, 14:00:00, 07/03/2018
    MD, 00:00:00, 05/28/2018
    MDWknd, 14:00:00, 05/25/2018
    GF, 00:00:00, 03/30/2018
    ES, 14:00:00, 03/29/2018
    PD, 00:00:00, 02/19/2018
    MLK, 00:00:00, 01/15/2018
    NYD, 00:00:00, 01/01/2018
    NYE, 14:00:00, 12/29/2017
    CH, 00:00:00, 12/25/2017
    CE, 14:00:00, 12/22/2017
    BF, 14:00:00, 11/24/2017
    TH, 00:00:00, 11/23/2017
    CD, 00:00:00, 10/09/2017
    LD, 00:00:00, 09/04/2017
    J4, 00:00:00, 07/04/2017
    J4E, 14:00:00, 07/03/2017
    MD, 00:00:00, 05/29/2017
    MDWknd, 14:00:00, 06/26/2017
    GF, 00:00:00, 04/14/2017
    ES, 14:00:00, 04/13/2017
    PD, 00:00:00, 02/20/2017
    MLK, 00:00:00, 01/16/2017
    NYD, 00:00:00, 01/02/2017
    NYE, 14:00:00, 12/30/2016
    CH, 00:00:00, 12/26/2016
    CE, 14:00:00, 12/23/2016
    BF, 14:00:00, 11/25/2016
    TH, 00:00:00, 11/24/2016
    VD, 00:00:00, 11/11/2016
    CD, 00:00:00, 10/10/2016
    LD, 00:00:00, 09/05/2016
    J4, 00:00:00, 07/04/2016
    J4Wknd, 14:00:00, 07/01/2016
    MD, 00:00:00, 05/30/2016
    MDWknd, 14:00:00, 05/27/2016
    GF, 00:00:00, 03/25/2016
    ES, 14:00:00, 03/24/2016
    PD, 00:00:00, 2/15/2016
    MLK, 00:00:00, 01/18/2016
    NYD, 00:00:00, 01/01/2016
    NYE, 14:00:00, 12/31/2015
 	CH, 00:00:00, 12/25/2015
 	CE, 14:00:00, 12/24/2015
 	BF, 14:00:00, 11/27/2015
 	TH, 00:00:00, 11/26/2015
 	VD, 00:00:00, 11/11/2015
 	CD, 00:00:00, 10/12/2015
 	LD, 00:00:00, 09/07/2015
 	J4, 00:00:00, 07/03/2015
 	J4E, 14:00:00, 07/02/2015
 	MD, 00:00:00, 05/25/2015
 	MDWknd, 14:00:00, 05/22/2015
 	GF, 14:00:00, 04/03/2015
 	ES, 14:00:00, 04/02/2015
 	PD, 00:00:00, 02/16/2015
 	MLK, 00:00:00, 01/19/2015
 	NYD, 00:00:00, 01/01/2015
 	NYE, 14:00:00, 12/31/2014
 	CH, 00:00:00, 12/25/2014
 	CE, 14:00:00, 12/24/2014
 	BF, 14:00:00, 11/28/2014
 	TH, 00:00:00, 11/27/2014
 	VD, 00:00:00, 11/11/2014
 	CD, 00:00:00, 10/13/2014
 	LD, 00:00:00, 09/01/2014
 	J4, 00:00:00, 07/04/2014
 	J4E, 14:00:00, 07/03/2014
 	MD, 00:00:00, 05/26/2014
 	MDWknd, 14:00:00, 05/23/2014
 	GF, 00:00:00, 04/18/2014
 	ES, 14:00:00, 04/17/2014
 	PD, 00:00:00, 02/17/2014
 	MLK, 00:00:00, 01/20/2014
 	NYD, 00:00:00, 01/01/2014
 	NYE, 14:00:00, 12/31/2013
 	CH, 00:00:00, 12/25/2013
 	CE, 14:00:00, 12/24/2013
 	BF, 14:00:00, 11/29/2013
 	TH, 00:00:00, 11/28/2013
 	VD, 00:00:00, 11/11/2013
 	CD, 00:00:00, 10/14/2013
 	LD, 00:00:00, 09/02/2013
 	J4, 00:00:00, 07/04/2013
 	MD, 00:00:00, 05/27/2013
 	MDWknd, 14:00:00, 05/24/2013
 	GF, 00:00:00, 03/29/2013
 	ES, 14:00:00, 03/28/2013
 	PD, 00:00:00, 02/18/2013
 	MLK, 00:00:00, 01/21/2013
 	NYD, 00:00:00, 01/01/2013
 	NYE, 14:00:00, 12/31/2012
 	CH, 00:00:00, 12/25/2012
 	CE, 14:00:00, 12/24/2012
 	BF, 14:00:00, 11/23/2012
 	TH, 00:00:00, 11/22/2012
 	VD, 00:00:00, 11/12/2012
 	Sandy, 00:00:00, 10/30/2012
 	Sandy, 12:00:00, 10/29/2012
 	CD, 00:00:00, 10/08/2012
 	LD, 00:00:00, 09/03/2012
 	J4, 00:00:00, 07/04/2012
 	MD, 00:00:00, 05/28/2012
 	MDWknd, 14:00:00, 05/25/2012
 	GF, 12:00:00, 04/06/2012
 	PD, 00:00:00, 02/20/2012
 	MLK, 00:00:00, 01/16/2012
 	NYD, 00:00:00, 01/02/2012
 	NYE, 14:00:00, 12/30/2011
;
run;

* Reformatting datetime info ;
data sifma;
set sifma;
date1 = input(date, MMDDYY10.);
closing_time1 = input(closing_time, hhmmss8.);
format date1 date9. closing_time1 time8.;
drop date closing_time;
rename date1 = date closing_time1 = closing_time;
run;

* Save created holiday schedule dataset ;
libname mylib "~/"; 		* ~/ is home directory;
data mylib.sifma; 
set sifma;
run;

* Identify execution dates that are SIFMA holidays ;
proc sql;
	create table _sifma_match as 
	select a.*, b.*
	from _trace_merged3 as a left join mylib.sifma as b 
	on a.trd_exctn_dt = b.date;
quit;

* Use matched datetime filter to delete flagged rows ;
data _trace_fisd1;
set _sifma_match;
outside_hrs = 0;
if trd_exctn_tm > closing_time then outside_hrs = 1;
delete_flag = 0;
if (not missing(name) and outside_hrs = 1) then delete_flag = 1;
if delete_flag = 1 then delete;
drop outside_hrs delete_flag name date closing_time;
run;

/* Step 5: Deal with Price Problems */

* Clean prices between $0 and $220 ;
data _trace_fisd2;
set _trace_fisd1;
if 0 < rptd_pr <= 220;
run;

* Sort data on CUSIP identifier and date-timestamp ;
proc sort data = _trace_fisd2 out = _trace_fisd_sort;
by cusip_id trd_exctn_dt trd_exctn_tm;
run;

* Lag price and date column ;
data _lags;
set _trace_fisd_sort;
by cusip_id;
date_last = lag(trd_exctn_dt);
prc_last = lag(rptd_pr);
if first.cusip_id then do;
	date_last = .;
	prc_last = .;
end;
format date_last YYMMDDN8.;
run;

* Sanity check / in case: sort again before proc expand ;
proc sort data=_lags out = _lags_sort;
by cusip_id trd_exctn_dt trd_exctn_tm;
run;

* Lead price and execution date ;
proc expand data = _lags out=_lags_leads method=none;
by cusip_id;
convert trd_exctn_dt = date_next / transformout = (lead 1);
convert rptd_pr = prc_next / transformout = (lead 1);
run;

* Get rid of extra column! ;
data _lags_leads;
set _lags_leads;
drop time;
run;

* Delete Pseudo- and Large Return Reversals ;
data _trace_fisd3;
set _lags_leads;
format date_next date_last YYMMDDN8. ;
days_last = trd_exctn_dt - date_last;
days_next = date_next - trd_exctn_dt;
delete_flag = 0;
if not cmiss(rptd_pr, prc_next, prc_last) and (prc_last>0) 
	and (rptd_pr ^= prc_last) and (rptd_pr ^= prc_next) then do;
		if (log(rptd_pr/prc_last) > 0.2 and log(prc_next/rptd_pr) < -0.2) 
		and (days_last<=7) and (days_next<=7) 
		then delete_flag=1;   
		if (log(rptd_pr/prc_last) < -0.2 and log(prc_next/rptd_pr) > 0.2) 
		and (days_last <= 7) and (days_next <= 7) 
		then delete_flag=1;	
end;
if delete_flag ^= 1;
drop delete_flag prc_last prc_next
days_last days_next date_last date_next;
run;

* Compute & Delete Large Deviations from Rolling Median ;
%macro roll_median_asquith(dsetin, dsetout);

* Just in case: sort again in ascending order (panel structure) ;
proc sort data = &dsetin. out = _sort;
by cusip_id trd_exctn_dt trd_exctn_tm;
run;

* Compute Rolling Median & lead/lag 20 execution date records ;
proc expand data = _sort out = _meds method = none;
by cusip_id;
convert rptd_pr = _prc_med_begin / transformout = (cmovmed 41);
convert trd_exctn_dt = _20_back / transformout = (lag 20);
convert trd_exctn_dt = _20_ahead / transformout = (lead 20);
run;

* Delete Deviations from Rolling Median, if not first/last 20 obs ;
data &dsetout.;
set _meds;
if (abs(rptd_pr - _prc_med_begin) >= 20 )
and (not missing(_20_back))
and (not missing(_20_ahead))
then delete; 
drop _prc_med_begin _20_back _20_ahead time;		
run;
%mend;

* Run macro twice, like Asquith code ;
%roll_median_asquith(_trace_fisd3, _temp);
%roll_median_asquith(_temp, _trace_fisd4);

******************************************************************;

******************************************************************;
/************* Part 6: Other Filters From Literature *************/

/* Additional guard: still 100s of random non-US corp bonds */

* Deletion set: gov bonds, convertible, asset-back, etc. ;
%let non_US_corp = ("ABS", "ADEB", "ASPZ", 
"CCOV", "FGOV", "PS", "RNT", "TPCS", "USBD");

* Deletion set: floating coupon types ;
%let float = ("CFFI", "CFFL", "CFLS", "CSFL", "F", "S");

/* Recent Literature Filtration */

* Maturity in [1, 30] yrs, USD, no odd coupons, etc. ;
data post_fisd_final;
set _trace_fisd4;
if not missing(offering_date);					* doesn't make sense to have inactive bond ;
if not missing(maturity);						* no perpetuities (Bessembinder 2009);
if 1 <= yrdif(stlmnt_dt, maturity, 'ACT/ACT') <= 30; 	* mature in 1-30 yrs ;
if coupon_change_indicator in &float. then delete;			* no floaters ; 
if interest_frequency in ("0" "1" "2" "4" "12");			* no odd frequency coupons ;
if foreign_currency = "N";						* only USD denominated ;
if putable = "Y" then delete;					* no putable bonds ;
if callable = "Y" then delete;					* no callable -- oof this a mean filter!!! ;
if sinking_fund = "Y" then delete;				* no sinking funds ;
if bond_type in &non_US_corp. then delete;		* US corp bonds only ;
run;

******************************************************************;

******************************************************************;
/**************** Part 7: Export Final Dataset *******************/

* Export cleaned merged dataset here ;
libname mylib "~/"; 		*~/ is home directory;
data mylib.clean_post_en_fisd; 
set post_fisd_final;
run;
******************************************************************;
