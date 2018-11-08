/*
这里有几点需要注意：
1）如何利用extract提取时间戳中的day hour minute等；
2）利用::numrric将extract提取的数字转换为double类型
3）利用round保留有效数字
*/
SELECT round(((extract(DAY FROM a.admittime - p.dob)
               + (extract(HOUR FROM a.admittime - p.dob)) / 24
               + (extract(MINUTE FROM a.admittime - p.dob)) / 60 / 24
              ) / 365.25) :: NUMERIC,4)
  AS age
FROM admissions a
  LEFT JOIN patients p
    ON a.subject_id = p.subject_id