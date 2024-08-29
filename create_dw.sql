CREATE EXTENSION postgis;

CREATE TABLE lines (
	line_id SMALLINT PRIMARY KEY
);

CREATE TABLE times (
	time_id INT PRIMARY KEY,
	datetime TIMESTAMP,
	day SMALLINT,
	month SMALLINT,
	year SMALLINT,
	hour SMALLINT,
	minute SMALLINT
);


CREATE TABLE stops (
	stop_id INT,
	direction BOOLEAN,
	stop_name TEXT,
	stop_sequence INT,
	line_id INT,
	geometry GEOMETRY,
	PRIMARY KEY (stop_id, direction, line_id)
);

DROP TABLE IF EXISTS segments;
CREATE TABLE segments (
	start_stop_id INT,
	end_stop_id INT,
	geometry GEOMETRY
);

CREATE TABLE speeds (
	start_stop_id INT,
	end_stop_id INT,
	time_id INT,
	avg_speed DECIMAL,
	median_speed DECIMAL,
	source TEXT,
	PRIMARY KEY (start_stop_id, end_stop_id, time_id, source)
);

COPY lines(line_id)
FROM '/Users/cookiefinder/Documents/BDMA/Summer_Internship_24/TrafficSegmentSpeedsBrussels/preprocessed_data/Lines.csv' DELIMITER ',' CSV HEADER;

COPY times(time_id,datetime,day,month,year,hour,minute)
FROM '/Users/cookiefinder/Documents/BDMA/Summer_Internship_24/TrafficSegmentSpeedsBrussels/preprocessed_data/Times.csv' DELIMITER ',' CSV HEADER;

COPY speeds(start_stop_id,end_stop_id,time_id,avg_speed,median_speed,source)
FROM '/Users/cookiefinder/Documents/BDMA/Summer_Internship_24/TrafficSegmentSpeedsBrussels/preprocessed_data/FactTable.csv' DELIMITER ',' CSV HEADER;

-- ogr2ogr -f "PostgreSQL" PG:"dbname=tssb_aug27 user=postgres password=password host=localhost" /Users/cookiefinder/Documents/BDMA/Summer_Internship_24/TrafficSegmentSpeedsBrussels/preprocessed_data/Stops.geojson -nln stops -overwrite -nlt POINT -lco GEOMETRY_NAME=geometry

SELECT * FROM stops;

-- ogr2ogr -f "PostgreSQL" PG:"dbname=tssb_aug27 user=postgres password=password host=localhost" /Users/cookiefinder/Documents/BDMA/Summer_Internship_24/TrafficSegmentSpeedsBrussels/preprocessed_data/Segments.geojson -nln segments -overwrite -nlt LINESTRING -lco GEOMETRY_NAME=geometry

SELECT * FROM segments;

ALTER TABLE segments DROP CONSTRAINT segments_pkey;

ALTER TABLE segments
ADD PRIMARY KEY (start_stop_id, end_stop_id);


ALTER TABLE stops
ADD CONSTRAINT fk_stops_line_id
FOREIGN KEY (line_id) REFERENCES lines(line_id);

ALTER TABLE speeds
ADD CONSTRAINT fk_speeds_start_stop_id
FOREIGN KEY (start_stop_id,end_stop_id) REFERENCES segments(start_stop_id, end_stop_id);

ALTER TABLE speeds
ADD CONSTRAINT fk_speeds_time_id
FOREIGN KEY (time_id) REFERENCES times(time_id);



