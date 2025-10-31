CREATE DATABASE library;
USE library;

CREATE TABLE staging_library_data (
    patron_type_code INT,
    patron_type_definition VARCHAR(25),
    total_checkouts INT,
    total_renewals INT,
    age_range VARCHAR(20),
    home_library_code VARCHAR(5),
    home_library_definition VARCHAR(50),
    circulation_active_month VARCHAR(9),
    circulation_active_year VARCHAR(10),
    notification_preference_code CHAR(1),
    notification_code_definition VARCHAR(10),
    provided_email_address VARCHAR(5),
    within_sf VARCHAR(5),
    year_patron_registered INT
);

SET GLOBAL local_infile=ON;

LOAD DATA LOCAL INFILE "SFPL_DataSF_library-usage_Jan_2023.csv"
INTO TABLE staging_library_data
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 12 ROWS
(@patron_type_code, 
@patron_type_definition,
@total_checkouts, 
@total_renewals,
@age_range,
@home_library_code, 
@home_library_definition, 
@circulation_active_month,
@circulation_active_year, 
@notification_preference_code, 
@notification_code_definition,
@provided_email_address, 
@within_sf, 
@year_patron_registered)
SET
 patron_type_code = NULLIF(TRIM(@patron_type_code), ''),
 patron_type_definition = NULLIF(TRIM(@patron_type_definition), ''),
 total_checkouts = NULLIF(TRIM(@total_checkouts), ''),
 total_renewals = NULLIF(TRIM(@total_renewals), ''),
 age_range = NULLIF(TRIM(@age_range), ''),
 home_library_code = NULLIF(TRIM(@home_library_code), ''),
 home_library_definition = NULLIF(TRIM(@home_library_definition), ''),
 circulation_active_month = NULLIF(TRIM(@circulation_active_month), ''),
 circulation_active_year = NULLIF(TRIM(@circulation_active_year), ''),
 notification_preference_code = NULLIF(TRIM(@notification_preference_code), ''),
 notification_code_definition = NULLIF(TRIM(@notification_code_definition), ''),
 provided_email_address = NULLIF(TRIM(@provided_email_address), ''),
 within_sf = NULLIF(TRIM(@within_sf), ''),
 year_patron_registered = NULLIF(TRIM(@year_patron_registered), '');
 

CREATE TABLE library_info (
    library_id INT AUTO_INCREMENT PRIMARY KEY,
    home_library_code VARCHAR(5) UNIQUE,
    home_library_definition VARCHAR(50),
    within_sf BOOLEAN
);

INSERT INTO library_info(
home_library_code, 
home_library_definition, 
within_sf)
SELECT
home_library_code,
MIN(TRIM(home_library_definition)) AS home_library_definition,
IF(MIN(TRIM(within_sf))='True', TRUE, FALSE) AS within_sf
FROM staging_library_data
GROUP BY home_library_code;

CREATE TABLE notification_preferences (
notification_code CHAR(1) PRIMARY KEY,
notification_definition VARCHAR(50)
);

INSERT INTO notification_preferences(
notification_code,
notification_definition
)
SELECT DISTINCT 
notification_preference_code, 
notification_code_definition
FROM staging_library_data
WHERE notification_preference_code IS NOT NULL;

CREATE TABLE patron_types (
patron_type_code INT PRIMARY KEY,
patron_type_definition VARCHAR(30)
);

INSERT INTO patron_types (
patron_type_code, 
patron_type_definition
)
SELECT DISTINCT 
patron_type_code, 
patron_type_definition
FROM staging_library_data
WHERE patron_type_code IS NOT NULL;


CREATE TABLE patrons (
patron_id INT AUTO_INCREMENT PRIMARY KEY,
patron_type_code INT,
total_checkouts INT,
total_renewals INT,
age_range VARCHAR(20),
home_library_code VARCHAR(5),
notification_preference_code CHAR(1),
provided_email_address BOOLEAN,
circulation_active_month VARCHAR(9),
circulation_active_year INT,
year_patron_registered INT,
FOREIGN KEY (patron_type_code) REFERENCES patron_types(patron_type_code),
FOREIGN KEY (home_library_code) REFERENCES library_info(home_library_code),
FOREIGN KEY (notification_preference_code) REFERENCES notification_preferences(notification_code)
);

INSERT INTO patrons(
patron_type_code, 
total_checkouts, 
total_renewals, 
age_range,
home_library_code, 
notification_preference_code, 
provided_email_address,
circulation_active_month, 
circulation_active_year, 
year_patron_registered
)
SELECT 
patron_type_code, 
total_checkouts,
total_renewals, 
age_range,
CASE 
	WHEN home_library_code IN (SELECT home_library_code FROM library_info) 
	THEN home_library_code 
	ELSE NULL 
END AS home_library_code,
CASE 
	WHEN notification_preference_code IN (SELECT notification_code FROM notification_preferences) 
	THEN notification_preference_code 
	ELSE NULL 
END AS notification_preference_code,
CASE 
	WHEN LOWER(TRIM(provided_email_address)) IN ('true', 'yes', '1') THEN TRUE
	WHEN LOWER(TRIM(provided_email_address)) IN ('false', 'no', '0') THEN FALSE
	ELSE NULL
END AS provided_email_address,
circulation_active_month, 
circulation_active_year, 
year_patron_registered
FROM staging_library_data;
 
CREATE TABLE demographic_summary (
demographic_id INT AUTO_INCREMENT PRIMARY KEY,
library_id INT,
age_range VARCHAR(20),
total_checkouts INT,
total_renewals INT,
FOREIGN KEY (library_id) REFERENCES library_info(library_id)
);

INSERT INTO demographic_summary(
library_id, age_range,
total_checkouts, 
total_renewals
)
SELECT 
l.library_id,
p.age_range, 
SUM(p.total_checkouts), 
SUM(p.total_renewals)
FROM patrons p
JOIN library_info l ON p.home_library_code = l.home_library_code
GROUP BY l.library_id, p.age_range;

CREATE TABLE library_mail_digital (
patron_id INT PRIMARY KEY,
patron_type_code INT,
age_range VARCHAR(20),
total_checkouts INT,
total_renewals INT,
FOREIGN KEY (patron_id) REFERENCES patrons(patron_id),
FOREIGN KEY (patron_type_code) REFERENCES patron_types(patron_type_code)
);

INSERT INTO library_mail_digital(
patron_id, 
patron_type_code,
age_range, 
total_checkouts, 
total_renewals
)
SELECT 
patron_id, 
patron_type_code, 
age_range, 
total_checkouts, 
total_renewals
FROM patrons
WHERE patron_type_code IN (12, 16);

CREATE TABLE staff_teachers (
patron_id INT PRIMARY KEY,
patron_type_code INT,
age_range VARCHAR(20),
home_library_code VARCHAR(5),
total_checkouts INT,
total_renewals INT,
FOREIGN KEY (patron_id) REFERENCES patrons(patron_id),
FOREIGN KEY (patron_type_code) REFERENCES patron_types(patron_type_code),
FOREIGN KEY (home_library_code) REFERENCES library_info(home_library_code)
);

INSERT INTO staff_teachers(
patron_id, 
patron_type_code, 
age_range, 
home_library_code, 
total_checkouts, 
total_renewals
)
SELECT 
patron_id, 
patron_type_code, 
age_range, 
home_library_code, 
total_checkouts, 
total_renewals
FROM patrons
WHERE patron_type_code IN (5, 15, 55);

drop table staging_library_data;
