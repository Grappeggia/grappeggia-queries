WITH params AS (
  SELECT 30 AS days_back, CURRENT_TIMESTAMP() AS end_ts
),
calls AS (
  SELECT
    g.call_start_time,
    g.transcript,
    g.gong_call_id,
    g.primary_sfdc_account_id,
    g.primary_sfdc_opportunity_id
  FROM DWH_PROD.PRISM.GONG_TRANSCRIPTS AS g
  JOIN params p ON TRUE
  WHERE NOT g.primary_sfdc_account_id IS NULL
    AND g.call_start_time >= DATEADD(DAY, -p.days_back, p.end_ts)
  QUALIFY ROW_NUMBER() OVER (ORDER BY g.call_start_time DESC) <= 20000
), contacts AS (
  SELECT
    g.gong_call_id,
    MAX(c.organization_name) AS account_name,
    ARRAY_AGG([c.primary_email, c.job_title, c.location]) WITHIN GROUP (ORDER BY c.primary_email) AS contact_info
  FROM calls g
  LEFT JOIN DWH_PROD.PRISM.common_room_contacts AS c
    ON g.primary_sfdc_account_id = c.sfdc_account_id
  GROUP BY g.gong_call_id
), cleaned AS (
  SELECT
    g.gong_call_id,
    PARSE_JSON(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          CAST(g.transcript AS TEXT),
          '\"(start|end|topic)\":\\s*\"?[^\",}]*\"?,?\\s*',
          ''
        ),
        ',\\s*}',
        '}'
      )
    ) AS transcript
  FROM calls g
), transcript_items AS (
  SELECT
    cl.gong_call_id,
    f.index AS item_index,
    f.value AS item
  FROM cleaned cl,
    LATERAL FLATTEN(input => cl.transcript) f
), sentence_texts AS (
  SELECT
    ti.gong_call_id,
    ti.item_index,
    ti.item:speakerId::string AS speakerId,
    LISTAGG(s.value:text::string, ' ') WITHIN GROUP (ORDER BY s.index) AS sentence_text
  FROM transcript_items ti,
    LATERAL FLATTEN(input => ti.item:sentences) s
  GROUP BY
    ti.gong_call_id,
    ti.item_index,
    speakerId
), transcript_agg AS (
  SELECT
    gong_call_id,
    ARRAY_AGG(
      OBJECT_CONSTRUCT(
        speakerId, sentence_text
      )
    ) WITHIN GROUP (ORDER BY item_index) AS transcript
  FROM sentence_texts
  GROUP BY gong_call_id
)
SELECT
  co.account_name,
  g.gong_call_id,
  TO_CHAR(g.call_start_time, 'YYYY-MM-DD HH24:MI') AS call_start_time,
  REGEXP_REPLACE(
    REGEXP_REPLACE(TO_JSON(ta.transcript), '[\\r\\n\\t\\f\\v]+', ' '),
    '\\s{2,}', ' '
  ) AS transcript,
  g.primary_sfdc_account_id,
  g.primary_sfdc_opportunity_id,
  REGEXP_REPLACE(
    REGEXP_REPLACE(TO_JSON(co.contact_info), '[\\r\\n\\t\\f\\v]+', ' '),
    '\\s{2,}', ' '
  ) AS contact_info
FROM calls g
LEFT JOIN contacts co
  ON co.gong_call_id = g.gong_call_id
LEFT JOIN transcript_agg ta
  ON ta.gong_call_id = g.gong_call_id
ORDER BY call_start_time DESC;