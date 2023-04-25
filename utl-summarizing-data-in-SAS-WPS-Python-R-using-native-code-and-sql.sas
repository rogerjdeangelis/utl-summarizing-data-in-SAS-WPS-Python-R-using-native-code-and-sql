%let pgm=utl-summarizing-data-in-SAS-WPS-Python-R-using-native-code-and-sql;

Summarizing data in SAS, WPS, Python, R  using native code and sql

Seven Solutions (SQL seems to be an afterthought in R and expecially python)

    1. SAS sql
    2. WPS sql
    3. SAS proc means
    4. WPS proc means
    5. R sql         (np outer join? good support for passthru?)
    6. Python sql    (no outer join?)
    7. R tidyverse
    8. Python native

SOAPBOX ON
  Really do not understand the popularity of Python
  Bizzare syntax
  Way too many data structures and data types
  Often difficult to get a dataframe
  Very poor support for sql
  It is a legend in its own mind
SOAPBOX OFF

github
https://tinyurl.com/ycktevua
https://github.com/rogerjdeangelis/utl-summarizing-data-in-SAS-WPS-Python-R-using-native-code-and-sql

his repo
https://tinyurl.com/9x9sp9vv
https://github.com/rogerjdeangelis/utl-python-r-and-sas-sql-solutions-to-add-missing-rows-to-a-data-table

Support for SQL is very poor in R and Python.
For instance outer joins are not supported in R or Python and most stat/math functions are
not directly supported in Python. unctions.

StackOverflow R
https://tinyurl.com/58mkv62d
https://stackoverflow.com/questions/73765595/conditional-insertion-of-extra-row-in-data-table

Solution by Akrun
https://stackoverflow.com/users/3732271/akrun

Related github
https://tinyurl.com/khkhkxxm
https://github.com/rogerjdeangelis/utl-sqlite-processing-in-python-with-added-math-and-stat-functions

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;

libname sd1 "d:/sd1";

data sd1.have;
 set sashelp.class(keep=sex weight);
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs from last table WORK.CLASS total obs=19 25APR2023:08:44:23                                                */
/*                                                                                                                        */
/* Obs    SEX    WEIGHT                                                                                                   */
/*                                                                                                                        */
/*   1     F       50.5                                                                                                   */
/*   2     F       77.0                                                                                                   */
/*   3     F       84.0                                                                                                   */
/*   4     M       83.0                                                                                                   */
/*   5     M       85.0                                                                                                   */
/*   6     M       99.5                                                                                                   */
/*   7     F       84.5                                                                                                   */
/*   8     F      112.5                                                                                                   */
/*   9     M       84.0                                                                                                   */
/*  10     F      102.5                                                                                                   */
/*  11     M      102.5                                                                                                   */
/*  12     F       90.0                                                                                                   */
/*  13     M      128.0                                                                                                   */
/*  14     F       98.0                                                                                                   */
/*  15     F      112.0                                                                                                   */
/*  16     M      112.0                                                                                                   */
/*  17     M      133.0                                                                                                   */
/*  18     M      112.5                                                                                                   */
/*  19     M      150.0                                                                                                   */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*           _               _
  ___  _   _| |_ _ __  _   _| |_
 / _ \| | | | __| `_ \| | | | __|
| (_) | |_| | |_| |_) | |_| | |_
 \___/ \__,_|\__| .__/ \__,_|\__|
                |_|
*/

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs from last table WORK.SQL_SAS total obs=2 25APR2023:09:08:16                                               */
/*                                                                                                                        */
/* Obs    SEX    WGTMIN     WGTAVG    WGTMID     WGTSTD    WGTMAX                                                         */
/*                                                                                                                        */
/*  1      F      50.5      90.111     90.00    19.3839     112.5                                                         */
/*  2      M      83.0     108.950    107.25    22.7272     150.0                                                         */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                         _
 ___  __ _ ___   ___  __ _| |
/ __|/ _` / __| / __|/ _` | |
\__ \ (_| \__ \ \__ \ (_| | |
|___/\__,_|___/ |___/\__, |_|
                        |_|
*/

proc sql;
  create
     table sql_sas as
  select
     sex
    ,min(weight )   as wgtMin
    ,mean(weight)   as wgtAvg
    ,median(weight) as wgtMid
    ,std(weight)    as wgtStd
    ,max(weight)    as wgtMax
  from
    sd1.have
  group
    by sex
;quit;

/*                              _
__      ___ __  ___   ___  __ _| |
\ \ /\ / / `_ \/ __| / __|/ _` | |
 \ V  V /| |_) \__ \ \__ \ (_| | |
  \_/\_/ | .__/|___/ |___/\__, |_|
         |_|                 |_|
*/

%utl_submit_wps64('
   options validvarname=any;
  libname sd1 "d:/sd1";
  proc sql;
  create
     table sql_sas as
  select
     sex
    ,min(weight )   as wgtMin
    ,mean(weight)   as wgtAvg
    ,median(weight) as wgtMid
    ,std(weight)    as wgtStd
    ,max(weight)    as wgtMax
  from
    sd1.have
  group
    by sex
;quit;
 proc print;
 run;quit;
');
/*
 ___  __ _ ___   _ __  _ __ ___   ___   _ __ ___   ___  __ _ _ __  ___
/ __|/ _` / __| | `_ \| `__/ _ \ / __| | `_ ` _ \ / _ \/ _` | `_ \/ __|
\__ \ (_| \__ \ | |_) | | | (_) | (__  | | | | | |  __/ (_| | | | \__ \
|___/\__,_|___/ | .__/|_|  \___/ \___| |_| |_| |_|\___|\__,_|_| |_|___/
                |_|
*/
proc means data=sd1.have min mean median std max;
class sex;
run;quit;

/*
__      ___ __  ___   _ __  _ __ ___   ___   _ __ ___   ___  __ _ _ __  ___
\ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __| | `_ ` _ \ / _ \/ _` | `_ \/ __|
 \ V  V /| |_) \__ \ | |_) | | | (_) | (__  | | | | | |  __/ (_| | | | \__ \
  \_/\_/ | .__/|___/ | .__/|_|  \___/ \___| |_| |_| |_|\___|\__,_|_| |_|___/
         |_|         |_|
*/
%utl_submit_wps64('
  libname sd1 "d:/sd1";
proc means data=sd1.have min mean median std max;
class sex;
run;quit;
');


/*                _
 _ __   ___  __ _| |
| `__| / __|/ _` | |
| |    \__ \ (_| | |
|_|    |___/\__, |_|
               |_|
*/

%utlfkil(d:/xpt/want.xpt);

%utl_submit_r64("
  library(haven);
  library(SASxport);
  library(sqldf);
  have<-read_sas('d:/sd1/have.sas7bdat');
  have;
  want<-sqldf('
  select
     sex
    ,min(weight )   as wgtMin
    ,avg(weight)    as wgtAvg
    ,median(weight) as wgtMid
    ,stdev(weight)  as wgtStd
    ,max(weight)    as wgtMax
  from
    have
  group
    by sex
  ');
   want;
   write.xport(want,file='d:/xpt/want.xpt');
");

libname xpt xport "d:/xpt/want.xpt";

proc print data=xpt.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* R log                                                                                                                  */
/*                                                                                                                        */
/*   SEX wgtMin    wgtAvg wgtMid   wgtStd wgtMax                                                                          */
/* 1   F   50.5  90.11111  90.00 19.38391  112.5                                                                          */
/* 2   M   83.0 108.95000 107.25 22.72719  150.0                                                                          */
/*                                                                                                                        */
/* Back to SAS                                                                                                            */
/*                                                                                                                        */
/* Obs    SEX    WGTMIN     WGTAVG    WGTMID     WGTSTD    WGTMAX                                                         */
/*                                                                                                                        */
/*   1     F      50.5      90.111     90.00    19.3839     112.5                                                         */
/*   2     M      83.0     108.950    107.25    22.7272     150.0                                                         */
/*                                                                                                                        */
/**************************************************************************************************************************/
/*                       _
 _ __  _   _   ___  __ _| |
| `_ \| | | | / __|/ _` | |
| |_) | |_| | \__ \ (_| | |
| .__/ \__, | |___/\__, |_|
|_|    |___/          |_|
*/

proc datasets lib=work kill nodetails nolist;
run;quit;

%utlfkil(d:/xpt/res.xpt);

%utl_pybegin;
parmcards4;
from os import path
import pandas as pd
import xport
import xport.v56
import pyreadstat
import numpy as np
import pandas as pd
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
have, meta = pyreadstat.read_sas7bdat("d:/sd1/have.sas7bdat")
print(have);
res = pdsql("""
  select
     sex
    ,min(weight )   as wgtMin
    ,avg(weight)    as wgtAvg
    ,median(weight) as wgtMid
    ,stdev(weight)  as wgtStd
    ,max(weight)    as wgtMax
  from
    have
  group
    by sex
""")
print(res);
ds = xport.Dataset(res, name='res')
with open('d:/xpt/res.xpt', 'wb') as f:
    xport.v56.dump(ds, f)
;;;;
%utl_pyend;

libname pyxpt xport "d:/xpt/res.xpt";

proc contents data=pyxpt._all_;
run;quit;

proc print data=pyxpt.res;
run;quit;

data res;
   set pyxpt.res;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* python log                                                                                                             */
/*                                                                                                                        */
/*   SEX  wgtMin      wgtAvg  wgtMid     wgtStd  wgtMax                                                                   */
/* 0   F    50.5   90.111111   90.00  19.383914   112.5                                                                   */
/* 1   M    83.0  108.950000  107.25  22.727186   150.0                                                                   */
/*                                                                                                                        */
/* Back to SAS                                                                                                            */
/*                                                                                                                        */
/* Obs    SEX    WGTMIN     WGTAVG    WGTMID     WGTSTD    WGTMAX                                                         */
/*                                                                                                                        */
/*   1     F      50.5      90.111     90.00    19.3839     112.5                                                         */
/*   2     M      83.0     108.950    107.25    22.7272     150.0                                                         */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*      _   _     _
 _ __  | |_(_) __| |_   ___   _____ _ __ ___  ___
| `__| | __| |/ _` | | | \ \ / / _ \ `__/ __|/ _ \
| |    | |_| | (_| | |_| |\ V /  __/ |  \__ \  __/
|_|     \__|_|\__,_|\__, | \_/ \___|_|  |___/\___|
                    |___/
*/

%utlfkil(d:/xpt/want.xpt);

%utl_submit_r64('
  library(haven);
  library(SASxport);
  library(dplyr);
  have<-read_sas("d:/sd1/have.sas7bdat");
  have;
  have %>% group_by(SEX) %>%
          summarize(
               n      = n()
              ,min    = min(WEIGHT)
              ,mean   = mean(WEIGHT)
              ,median = median(WEIGHT)
              ,sd     = sd(WEIGHT)
              ,max    = max(WEIGHT)
          ) -> want;
   want<-as.data.frame(want);
   want;
   write.xport(want,file="d:/xpt/want.xpt");
');

libname xpt xport "d:/xpt/want.xpt";

proc print data=xpt.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  R                                                                                                                     */
/*                                                                                                                        */
/*    SEX  n  min      mean median       sd   max                                                                         */
/*  1   F  9 50.5  90.11111  90.00 19.38391 112.5                                                                         */
/*  2   M 10 83.0 108.95000 107.25 22.72719 150.0                                                                         */
/*                                                                                                                        */
/*  Back to SAS                                                                                                           */
/*                                                                                                                        */
/*  Obs    SEX     N     MIN      MEAN     MEDIAN       SD       MAX                                                      */
/*                                                                                                                        */
/*    1     F      9    50.5     90.111     90.00    19.3839    112.5                                                     */
/*    2     M     10    83.0    108.950    107.25    22.7272    150.0                                                     */
/*                                                                                                                        */
/**************************************************************************************************************************/
/*           _   _                               _   _
 _ __  _   _| |_| |__   ___  _ __    _ __   __ _| |_(_)_   _____
| `_ \| | | | __| `_ \ / _ \| `_ \  | `_ \ / _` | __| \ \ / / _ \
| |_) | |_| | |_| | | | (_) | | | | | | | | (_| | |_| |\ V /  __/
| .__/ \__, |\__|_| |_|\___/|_| |_| |_| |_|\__,_|\__|_| \_/ \___|
|_|    |___/
*/

%utlfkil(d:/xpt/want_py.xpt);

%utl_submit_py64_310('
from os import path;
import pandas as pd;
import xport;
import xport.v56;
import pyreadstat;
import numpy as np;
import pandas as pd;
import statistics as stat;
have, meta = pyreadstat.read_sas7bdat("d:/sd1/have.sas7bdat");
print(have);
want=have.groupby("SEX").agg(
        wgtmin=pd.NamedAgg(column="WEIGHT", aggfunc="min"),
        wgtavg=pd.NamedAgg(column="WEIGHT", aggfunc=np.mean),
        wgtmid=pd.NamedAgg(column="WEIGHT", aggfunc="median"),
        wgtstdev=pd.NamedAgg(column="WEIGHT", aggfunc=stat.stdev),
        wgtmax=pd.NamedAgg(column="WEIGHT", aggfunc="max"),
    );
print(want);
ds = xport.Dataset(want, name="want");
with open("d:/xpt/want_py.xpt", "wb") as f:;
.   xport.v56.dump(ds, f);
want.info();
');

libname xpt xport "d:/xpt/want_py.xpt";

proc print data=xpt.want;
run;quit;


/**************************************************************************************************************************/
/*                                                                                                                        */
/* Python                                                                                                                 */
/*                                                                                                                        */
/*        wgtmin      wgtav      wgtmid   wgtstdev  wgtmax                                                                */
/*   SEX                                                                                                                  */
/*   F      50.5   90.111111      90.00  19.383914   112.5                                                                */
/*   M      83.0  108.950000     107.25  22.727186   150.0                                                                */
/*                                                                                                                        */
/*  Bsck to SAS                                                                                                           */
/*                                                                                                                        */
/*    Obs    WGTMIN     WGTAVG    WGTMID    WGTSTDEV    WGTMAX                                                            */
/*                                                                                                                        */
/*      1     50.5      90.111     90.00     19.3839     112.5                                                            */
/*      2     83.0     108.950    107.25     22.7272     150.0                                                            */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
