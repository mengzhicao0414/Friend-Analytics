/*
2.a) Task a
Write a job(s) that reports all Facebook users (name, and hobby)
whose Nationality is the same as your own Nationality (pick one).
Note that nationalities in the data file are a random sequence of characters
unless you work with meaningful strings like Chinese or German. This is up to you.).
*/

Mpg = LOAD 'hdfs://localhost:9000/user/MyPage.txt' USING PigStorage(',') AS (f1:long,f2:chararray,f3:chararray,f4:long,f5:chararray);
AA = FILTER Mpg BY f3 == 'XantOEcWqctKBgLh';
Xa = FOREACH AA GENERATE $1, $4;

DUMP Xa;
STORE Xa INTO 'Xa_OUT';


/*
2.b) Task b
Write job(s) that reports for each country,
how many of its citizens have a Facebook page.
*/

Mpg = LOAD 'hdfs://localhost:9000/user/MyPage.txt' USING PigStorage(',') AS (f1:long,f2:chararray,f3:chararray,f4:long,f5:chararray);
BB = GROUP Mpg BY f3;
Xb = FOREACH BB GENERATE group, COUNT(Mpg.f1);

DUMP Xb;
STORE Xb INTO 'Xb_OUT';


/*
2.c) Task c
Find the top 10 interesting Facebook pages, namely,
those that got the most accesses based on your AccessLog dataset compared to all other pages.
*/

Fr = LOAD 'hdfs://localhost:9000/user/AccessTime.txt' USING PigStorage(',');
CC = GROUP Fr BY $2;
C_group = FOREACH CC GENERATE group, COUNT(Fr.$0);
C_order = ORDER C_group BY $1 DESC;

Xc = LIMIT C_order 10;

DUMP Xc;
STORE Xc INTO 'Xc_OUT';


/*
2.d) Task d
For each Facebook page, compute the “happiness factor” of its owner.
That is, for each Facebook page in your dataset, report the owner’s name,
and the number of people listing him or her as friend.
*/

Fr = LOAD 'hdfs://localhost:9000/user/Friends.txt' USING PigStorage(',') AS (f1:long,f2:long,f3:long,f4:chararray,f5:chararray);
DD = GROUP Fr BY f3;
D_group = FOREACH DD GENERATE group as group_id, COUNT(Fr.f1) AS number;
Mpg = LOAD 'hdfs://localhost:9000/user/MyPage.txt' USING PigStorage(',') AS (f1:long,f2:chararray,f3:chararray,f4:long,f5:chararray);
Mpg_forjoin = FOREACH Mpg GENERATE $0 ,$1;
D_join = JOIN D_group BY $0, Mpg_forjoin BY $0;
Xd = FOREACH D_join GENERATE $3 ,$1;

DUMP Xd;

STORE Xd INTO 'Xd_OUT';


/*
2.e) Task e
Determine which people have favorites. That is, for each Facebook page owner,
determine how many total accesses to Facebook pages they have made
(as reported in the AccessLog) and how many distinct Facebook pages they have accessed in total.
*/

Ass = LOAD 'hdfs://localhost:9000/user/AccessTime.txt' USING PigStorage(',');
EE = GROUP Ass BY $1;
Xe1 = FOREACH EE GENERATE group, COUNT(Ass.$0);
EE_12 = FOREACH Ass GENERATE $1, $2;
EE_dis = DISTINCT EE_12;
EE_group  = GROUP EE_dis BY $0;
Xe2 = FOREACH EE_group GENERATE $0, COUNT($1);

DUMP Xe1;
DUMP Xe2;

STORE Xe1 INTO 'Xe1_OUT';
STORE Xe2 INTO 'Xe2_OUT';


/*
2.g) Task f
Find the list of all people that have set up a Facebook page, but have lost interest,
i.e., after some initial time unit (say 10 days or whatever you choose)
have never accessed Facebook again (meaning no entries in the Facebook AccessLog exist
after that date).

*/

Ass = LOAD 'hdfs://localhost:9000/user/AccessTime.txt' USING PigStorage(',');
FF = GROUP Ass BY $1;
FF_f = FOREACH FF GENERATE $0, (MAX(Ass.$4) - MIN(Ass.$4));
FF_fi  = FILTER FF_f BY $1 < 900000;
Xf = FOREACH FF_fi GENERATE $0;

DUMP Xf;
STORE Xf INTO 'Xf_OUT';


/*
2.f) Task g
Identify people that have declared someone as their friend yet
who have never accessed their respective friend’s Facebook page
– indicating that they don’t care enough to find out any news about their friend
(at least not via Facebook).
*/

/*
Friends.PersonID($1), Friends.MyFriend($2),
AccessLog.ByWho($1), AccessLog.WhatPage($2), AccessLog.AccessId($0)
*/

Fr = LOAD 'hdfs://localhost:9000/user/Friends.txt' USING PigStorage(',') AS (f1:long,f2:long,f3:long,f4:chararray,f5:chararray);
Ass = LOAD 'hdfs://localhost:9000/user/AccessTime.txt' USING PigStorage(',');
G_Fr = FOREACH Fr GENERATE $1, $2;
G_Ass = FOREACH Ass GENERATE $1, $2, $0;

G_join = JOIN G_Fr BY ($0,$1) LEFT OUTER, G_Ass BY ($0,$1);
G_Fl = FILTER G_join BY $4 is null;
G_Pr = FOREACH G_Fl GENERATE $0;
Xg = DISTINCT G_Pr;

DUMP Xg;
STORE Xg INTO 'Xg_OUT';


/*
2.h) Task h
Report all owners of a Facebook who are famous and happy, namely,
those who have more friends than the average number of friends
across all owners in the data files.
*/

Mpg = LOAD 'hdfs://localhost:9000/user/MyPage.txt' USING PigStorage(',') AS (f1:long,f2:chararray,f3:chararray,f4:long,f5:chararray);
Fr = LOAD 'hdfs://localhost:9000/user/Friends.txt' USING PigStorage(',') AS (f1:long,f2:long,f3:long,f4:chararray,f5:chararray);

HH_mpg = FOREACH Mpg GENERATE $0;

HH_g = GROUP Fr BY $1;
HH_gf = FOREACH HH_g GENERATE group, COUNT(Fr.$2);

HH_join = JOIN HH_mpg BY $0 LEFT OUTER, HH_gf BY $0;

HH_groupall = Group HH_join all;

HH_avg = foreach HH_groupall Generate (SUM(HH_join.$2)  / COUNT(HH_join.$0));

HH_crossdata = CROSS HH_avg, HH_join;

HH_cd_fl  = FILTER HH_crossdata BY $3 > $0;

Xh = foreach HH_cd_fl Generate $1;

DUMP Xh;
STORE Xh INTO 'Xh_OUT';


/*
end
*/
