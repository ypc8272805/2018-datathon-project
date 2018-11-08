--这个可以用于提取患者的相关的生理参数，可以按照透视图的形式进行展示，
SELECT
  c.subject_id,
  c.hadm_id,
  c.icustay_id,
  CASE WHEN itemid IN (220277, 646)
    THEN 'SpO2'
  ELSE NULL END AS label,
  icu.intime,icu.outtime,c.charttime,c.itemid,
  CASE WHEN itemid IN (220277, 646) AND valuenum>0 AND valuenum<=100 THEN valuenum  ELSE null END AS valuenum
FROM mimiciii.icustay_detail icu
  LEFT JOIN mimiciii.chartevents c
  ON c.icustay_id=icu.icustay_id AND c.charttime BETWEEN icu.intime AND icu.outtime
AND c.itemid IN (220277, 646)


--提取患者某一时间段的生理参数的最大值、最小值、平均值，这样聚合的结果展示
--这个方法很有用
SELECT pvt.subject_id,pvt.hadm_id,pvt.icustay_id
,min(CASE WHEN VitalID=1 THEN valuenum ELSE null END ) AS HearteRate_min
FROM (
  SELECT ie.subject_id,ie.hadm_id,ie.icustay_id
  ,CASE
    WHEN itemid IN (211,220045) AND valuenum>0 AND valuenum<300 then 1--hearte rate
    ELSE null END AS VitalID,
valuenum
  FROM icustays ie
  LEFT JOIN chartevents ce
    ON ie.subject_id=ce.subject_id AND ie.hadm_id=ce.hadm_id AND ie.icustay_id=ce.icustay_id
  AND ce.charttime BETWEEN ie.intime AND ie.intime+INTERVAL '1' DAY
  AND ce.error IS DISTINCT FROM 1
  WHERE ce.itemid IN (
    211,--heart rate
    220045
  )
) pvt
GROUP BY pvt.subject_id,pvt.hadm_id,pvt.icustay_id
