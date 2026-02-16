--Schema Design
CREATE TABLE physicians (
    physician_id VARCHAR(20) PRIMARY KEY,
    physician_name VARCHAR(100),
    specialty VARCHAR(50),
    hospital VARCHAR(50),years_experience INT CHECK (years_experience BETWEEN 0 AND 60)
);
SELECT * FROM physicians

CREATE TABLE scribes (
    scribe_id VARCHAR(20) PRIMARY KEY,
    scribe_name VARCHAR(100),
    experience_level VARCHAR(50),
	shift VARCHAR(50)
  );
SELECT*FROM scribes
  
CREATE TABLE clinical_notes (
    note_id VARCHAR(50) PRIMARY KEY,
    scribe_id VARCHAR(50),
	physician_id VARCHAR(50),
    visit_date TIMESTAMP,
	note_time_minutes INT,
	note_status VARCHAR(150),
	
FOREIGN KEY (physician_id) REFERENCES physicians(physician_id),
FOREIGN KEY (scribe_id) REFERENCES scribes(scribe_id)
);

SELECT* FROM clinical_notes

CREATE TABLE qa_reviews (
    qa_id VARCHAR (30) PRIMARY KEY,
    note_id VARCHAR (30),
    qa_score DECIMAL(5,2),
    error_count INT,compliance_flag VARCHAR(10),
	FOREIGN KEY (note_id) REFERENCES clinical_notes(note_id) 

);
SELECT*FROM qa_reviews

--Schema Audit
SELECT
    rel_t.relname      AS table_name,
    att.attname        AS fk_column,
    rel_rt.relname     AS referenced_table,
    att_rt.attname     AS referenced_column,
    con.conname        AS constraint_name
FROM pg_constraint con
JOIN pg_class rel_t       ON rel_t.oid = con.conrelid
JOIN pg_attribute att    ON att.attrelid = rel_t.oid
                         AND att.attnum = ANY (con.conkey)
JOIN pg_class rel_rt     ON rel_rt.oid = con.confrelid
JOIN pg_attribute att_rt ON att_rt.attrelid = rel_rt.oid
                         AND att_rt.attnum = ANY (con.confkey)
WHERE con.contype = 'f'
ORDER BY rel_t.relname;
																				
 --Due to fixing csv import issues in operational stage find the REAL FK constraint name of the corresponding table.Then drop FK transiently and re add FK again after importing csv files.
---Find the FK of table clinical_notes and drop the FK
SELECT
    conname AS constraint_name
FROM
    pg_constraint
WHERE
    conrelid = 'clinical_notes'::regclass
    AND contype = 'f';

--Drop the foreign keys(FK) of table clinical_notes
ALTER TABLE clinical_notes
DROP CONSTRAINT fk_clinical_physician;
ALTER TABLE clinical_notes
DROP CONSTRAINT fk_clinical_scribe;
------Find the FK of table qa_reviews and drop the FK
SELECT conname
FROM pg_constraint
WHERE conrelid = 'qa_reviews'::regclass
  AND contype = 'f';
  
ALTER TABLE qa_reviews
DROP CONSTRAINT fk_qa_note;


--Load Data
---Data loaded manually with psql GUI .First loaded two master dataset physicians.csv and scribes.csv.Then
--two transection dataset clinical_notes.csv and qa_reviews.csv.

--Data Profiling & Cleaning
---Detect Foreign Key Mismatch
----physician FK Violations

SELECT DISTINCT physician_id
FROM clinical_notes
WHERE  physician_id IS NOT null
EXCEPT
SELECT physician_id
FROM physicians;

----scribe FK Violations

SELECT DISTINCT scribe_id
FROM clinical_notes
WHERE   scribe_id IS NOT null
EXCEPT
SELECT  scribe_id
FROM scribes;

----note FK Violations

SELECT note_id
FROM   qa_reviews
WHERE  note_id IS NOT null
EXCEPT
SELECT note_id
FROM clinical_notes;

--Detect invalid statuses
SELECT DISTINCT note_status
FROM clinical_notes
WHERE note_status NOT IN ('Completed','Pending','QA_Rejected')
  OR  note_status IS null;

---Data quality rule for note_status

ALTER TABLE clinical_notes
ADD CONSTRAINT note_status_chk CHECK (NOTE_STATUS IN ('Completed','Pending','QA_Rejected'));
																																																																												
																																																																																	
																																																																																	
																																																																																	
--qa_score Range Validation
---Detect Invalid qa_score
SELECT qa_id,
	qa_score
FROM qa_reviews
WHERE  qa_score < 0
	OR qa_score > 100
	OR qa_score IS null;

---Data quality rule for qa_score

ALTER TABLE qa_reviews
ADD CONSTRAINT qa_score_chk CHECK (qa_score BETWEEN 0 AND 100);

---NULL Value Checks
----Detect Null

SELECT *
FROM clinical_notes
WHERE  physician_id IS null
  OR   scribe_id    IS null
  OR   note_status  IS null;

---Data Quality Rule for Null Values

ALTER TABLE clinical_notes
ALTER COLUMN physician_id
SET NOT null,
ALTER COLUMN scribe_id
SET NOT null,
ALTER COLUMN note_status
SET NOT null;

---Duplicate Detection
----Duplicate notes

SELECT note_id,
	COUNT(*)
FROM clinical_notes
GROUP BY note_id
HAVING COUNT(*) > 1;

---Duplicate qa_review per Note

SELECT note_id,
	COUNT(*)
FROM qa_reviews
GROUP BY note_id
HAVING COUNT(*) > 1;

---Remove Duplicate Keep One Note per note_id

DELETE
FROM QA_REVIEWS
WHERE QA_ID NOT IN
		(SELECT QA_ID
			FROM
				(SELECT QA_ID,
						ROW_NUMBER() OVER (PARTITION BY NOTE_ID
																									ORDER BY INSERTED_AT DESC) AS RN
					FROM QA_REVIEWS) T
			WHERE RN = 1 );

----Enforce Uniqueness

CREATE UNIQUE INDEX ux_qa_note ON qa_reviews (note_id);

--SLA Calculations
---Add sla Column

ALTER TABLE clinical_notes 
ADD COLUMN   sla_status VARCHAR(30);

---Calculte sla (24 hours =1440 minutes)
UPDATE clinical_notes
SET sla_status =
CASE
    WHEN note_time_minutes <= 480 THEN 'SLA_Met'
    ELSE 'SLA_Breached'
END;


SELECT *
FROM clinical_notes

---Audit Timestamps
----Insert Timestamps

ALTER TABLE clinical_notes ADD COLUMN inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;


ALTER TABLE qa_reviews ADD COLUMN inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

---Update Timestamp

ALTER TABLE clinical_notes ADD COLUMN updated_at TIMESTAMP 
--Rejection Logging
---Create Rejection Logging

CREATE TABLE data_rejection_log (
    table_name VARCHAR(50),
    record_id VARCHAR(50),
    rejection_reason TEXT,
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
---Log rejected notes

INSERT INTO data_rejection_log (table_name, record_id, rejection_reason)
SELECT
    'clinical_notes',
    note_id,
    'Invalid note_status'
FROM clinical_notes
WHERE note_status NOT IN ('Completed', 'Pending', 'QA_Rejected');
																						
SELECT*FROM data_rejection_log
---Log invalid QA scores

INSERT INTO data_rejection_log (table_name, record_id, rejection_reason)
SELECT
    'qa_reviews',
    qa_id,
    'QA score out of range'
FROM qa_reviews
WHERE qa_score < 0 OR qa_score > 100;

---Log duplicate records

INSERT INTO data_rejection_log (table_name, record_id, rejection_reason)
SELECT
    'qa_reviews',
    qa_id,
    'Duplicate QA review for same note_id'
FROM (
    SELECT
        qa_id,
        ROW_NUMBER() OVER (
            PARTITION BY note_id
            ORDER BY inserted_at DESC
        ) AS rn
    FROM qa_reviews
) t
WHERE rn > 1;

--Re Add Foreign Keys
ALTER TABLE clinical_notes
ADD CONSTRAINT fk_clinical_physician
FOREIGN KEY (physician_id)
REFERENCES physicians (physician_id);

ALTER TABLE clinical_notes
ADD CONSTRAINT fk_clinical_scribe
FOREIGN KEY (scribe_id)
REFERENCES scribes (scribe_id);

ALTER TABLE qa_reviews
ADD CONSTRAINT fk_qa_note
FOREIGN KEY (note_id)
REFERENCES clinical_notes (note_id);

---Performance Optimization
CREATE INDEX idx_clinical_physician
ON clinical_notes (physician_id);

CREATE INDEX idx_clinical_scribe
ON clinical_notes (scribe_id);

CREATE INDEX idx_qa_note
ON qa_reviews (note_id);

---SLA & Productivity Analytics
 
----1.Which physicians are driving SLA penalties or client dissatisfaction?(KPI 1: SLA Breach Rate)
CREATE VIEW v_sla_breach_rate AS
SELECT
    physician_id,
    ROUND(
        100.0 * SUM(CASE WHEN sla_status = 'SLA_Breached' THEN 1 ELSE 0 END)
        / COUNT(*),
        2
    ) AS sla_breach_pct
FROM clinical_notes
GROUP BY physician_id;

SELECT*FROM v_sla_breach_rate
----In this stage v_sla_breach_rate from data output represent that  all the physician_id sla_breach_pct=0.Then I tried some sanity check to explore the root cause. 
--Sanity Checks
---Check if any notes exceed SLA
SELECT COUNT(*) AS breached_notes
FROM clinical_notes
WHERE note_time_minutes > 1440;

---Check time distribution
SELECT
    MIN(note_time_minutes),
    MAX(note_time_minutes),
    AVG(note_time_minutes)
FROM clinical_notes;
---Simulate SLA breaches (5–10%)
UPDATE clinical_notes
SET note_time_minutes = note_time_minutes + 800
WHERE note_id IN (
    SELECT note_id
    FROM clinical_notes
    ORDER BY RANDOM()
    LIMIT (SELECT COUNT(*) * 0.1 FROM clinical_notes)
);
--Recalculate sla_status
UPDATE clinical_notes
SET sla_status =
CASE
    WHEN note_time_minutes <= 1440 THEN 'SLA_Met'
    ELSE 'SLA_Breached'
END;
--Correct v_sla_breach_rate View
CREATE OR REPLACE VIEW v_sla_breach_rate AS
SELECT
    physician_id,
    COUNT(*) AS total_notes,
    SUM(CASE WHEN sla_status = 'SLA_Breached' THEN 1 ELSE 0 END) AS breached_notes,
    ROUND(
        100.0 * SUM(CASE WHEN sla_status = 'SLA_Breached' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS breach_rate_pct
FROM clinical_notes
GROUP BY physician_id
ORDER BY breach_rate_pct DESC ;
SELECT*FROM v_sla_breach_rate
----2.Which physicians consistently meet documentation SLAs?
-----SLA Performance by Physician
CREATE VIEW v_physician_sla AS
SELECT
    physician_id,
    COUNT(*) AS total_notes,
    SUM(CASE WHEN sla_status = 'SLA_Met' THEN 1 ELSE 0 END) AS sla_met_notes,
    ROUND(
        100.0 * SUM(CASE WHEN sla_status = 'SLA_Met' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS sla_compliance_pct
FROM clinical_notes
GROUP BY physician_id;
SELECT * FROM v_physician_sla
--3.Who produces the most valuable documentation, not just the most?
CREATE VIEW v_quality_adjusted_productivity AS
SELECT
    c.physician_id,
    COUNT(*) AS total_notes,
    ROUND(AVG(q.qa_score),2) AS avg_qa_score,
    ROUND(COUNT(*) * AVG(q.qa_score) / 100, 2) AS quality_adjusted_output
FROM clinical_notes c
JOIN qa_reviews q ON c.note_id = q.note_id
GROUP BY c.physician_id
ORDER BY quality_adjusted_output DESC
LIMIT 5;
DROP VIEW IF EXISTS v_quality_adjusted_productivity 
SELECT*FROM v_quality_adjusted_productivity
--4.What percentage of documentation requires costly rework?(KPI:2 Rework Rate)
CREATE VIEW v_rework_rate AS
SELECT
    COUNT(DISTINCT q.note_id) * 1.0
    / (SELECT COUNT(*) FROM clinical_notes) AS rework_ratio
FROM qa_reviews q
WHERE q.qa_score < 85;

SELECT*FROM v_rework_rate
---5.Which physicians are borderline and may breach SLA soon?
CREATE VIEW v_sla_risk_bucket AS
SELECT
    physician_id,
    CASE
        WHEN note_time_minutes <= 1000 THEN 'Low Risk'
        WHEN note_time_minutes <= 1440 THEN 'Medium Risk'
        ELSE 'High Risk'
    END AS sla_risk_level,
    COUNT(*) AS note_count
FROM clinical_notes
GROUP BY physician_id, sla_risk_level;
SELECT*FROM v_sla_risk_bucket
---Daily Completion Trend
---6.Is documentation throughput improving over time?
CREATE VIEW v_daily_completion_trend AS
SELECT
    DATE(inserted_at) AS work_date,
    COUNT(*) AS completed_notes
FROM clinical_notes
WHERE note_status = 'Completed'
GROUP BY DATE(inserted_at)
ORDER BY work_date;
SELECT*FROM v_daily_completion_trend

---In this stage query ran succesfully but only one row affected.so I tried to detect why this happened.
--with below step
SELECT MIN(inserted_at), MAX(inserted_at) FROM clinical_notes;
---From data output i detected that all data was batch-loaded, all rows share the same insert timestamp, resulting in a single grouped date.so I fixed this issue consider visit date as inserted date. 
UPDATE clinical_notes
SET inserted_at = visit_date;
--7.Which physician–scribe combinations produce the best outcomes?
CREATE VIEW v_physician_scribe_quality AS
SELECT
    physician_id,
    scribe_id,
    ROUND(AVG(q.qa_score), 2) AS avg_qa_score,
    AVG(c.note_time_minutes) AS avg_time
FROM clinical_notes c
JOIN qa_reviews q ON c.note_id = q.note_id
GROUP BY physician_id, scribe_id
ORDER BY avg_qa_score DESC;
SELECT*FROM v_physician_scribe_quality
----QA & Quality Analytics
----8.Which physicians generate higher-quality documentation?
-----QA Quality by Physician
CREATE VIEW v_physician_qa_quality AS
SELECT
    c.physician_id,
    ROUND(AVG(q.qa_score), 2) AS avg_qa_score,
    AVG(q.error_count) AS avg_error_count
FROM clinical_notes c
JOIN qa_reviews q ON c.note_id = q.note_id
GROUP BY c.physician_id;
SELECT*FROM v_physician_qa_quality

---High risk physicians
---9.Who needs intervention or retraining?
CREATE VIEW v_high_risk_physicians AS
WITH metrics AS (
    SELECT
        c.physician_id,
        AVG(q.qa_score) AS avg_qa_score,
        SUM(CASE WHEN c.sla_status = 'SLA_Met' THEN 1 ELSE 0 END) * 1.0
        / COUNT(*) AS sla_rate
    FROM clinical_notes c
    JOIN qa_reviews q ON c.note_id = q.note_id
    GROUP BY c.physician_id
	
)
SELECT *
FROM metrics
WHERE avg_qa_score < 85
  AND sla_rate < 0.80;
DROP VIEW IF EXISTS v_high_risk_physicians  
SELECT*FROM v_high_risk_physicians  

--Scribe Productivity Analytics
---10.Which scribes are most efficient without sacrificing quality?
-----scribes productivity & quality
CREATE VIEW v_scribe_performance AS
SELECT
    cn.scribe_id,
    COUNT(*) AS total_notes,
    ROUND(AVG(cn.note_time_minutes), 2) AS avg_note_time,
    ROUND(AVG(q.qa_score), 2) AS avg_qa_score
FROM clinical_notes cn
JOIN qa_reviews q ON cn.note_id = q.note_id
GROUP BY cn.scribe_id;

SELECT*FROM v_scribe_performance

---Top Performing Scribe per Day
---11.Who are the daily top contributors?
CREATE VIEW v_top_scribe_daily AS
WITH daily AS (
    SELECT
        scribe_id,
        DATE(inserted_at) AS work_date,
        COUNT(*) AS completed_notes
    FROM clinical_notes
    WHERE note_status = 'Completed'
    GROUP BY scribe_id, DATE(inserted_at)
)
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY work_date
               ORDER BY completed_notes DESC
           ) AS rn
    FROM daily
) t
WHERE rn = 1;
SELECT*FROM v_top_scribe_daily

---System Health & Governance Analytics
-----12.How healthy is the pipeline?
----Data quality dashboard view
CREATE VIEW v_data_quality_summary AS
SELECT
    (SELECT COUNT(*) FROM data_rejection_log) AS total_rejections,
    (SELECT COUNT(*) FROM qa_reviews) AS total_qa_records,
    (SELECT COUNT(*) FROM clinical_notes) AS total_notes;
SELECT*FROM v_data_quality_summary

--13.Rejection Trend
CREATE VIEW v_rejection_trend AS
SELECT
    DATE(logged_at) AS log_date,
    COUNT(*) AS rejection_count
FROM data_rejection_log
GROUP BY DATE(logged_at)
ORDER BY log_date;
SELECT*FROM v_rejection_trend

--14.Can leadership trust the data pipeline?
--Data Trust Score
CREATE VIEW v_data_trust_score AS
SELECT
    100.0 -
    (SELECT COUNT(*) FROM data_rejection_log) * 100.0
    / (SELECT COUNT(*) FROM clinical_notes) AS data_trust_pct;
SELECT*FROM v_data_trust_score