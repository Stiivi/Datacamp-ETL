CREATE TABLE etl_batches (
  id int(11) NOT NULL AUTO_INCREMENT,
  batch_type varchar(255) DEFAULT NULL,
  batch_source varchar(255) DEFAULT NULL,
  data_source_name varchar(255) DEFAULT NULL,
  data_source_url varchar(255) DEFAULT NULL,
  valid_due_date date DEFAULT NULL,
  batch_date date DEFAULT NULL,
  username varchar(255) DEFAULT NULL,
  created_at datetime DEFAULT NULL,
  updated_at datetime DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB AUTO_INCREMENT=237 DEFAULT CHARSET=utf8;

CREATE TABLE etl_job_status (
  id int(11) NOT NULL AUTO_INCREMENT,
  job_name varchar(255) DEFAULT NULL,
  job_id int(11) DEFAULT NULL,
  status varchar(255) DEFAULT NULL,
  phase varchar(255) DEFAULT NULL,
  message varchar(255) DEFAULT NULL,
  start_date datetime DEFAULT NULL,
  end_date datetime DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB AUTO_INCREMENT=1451 DEFAULT CHARSET=utf8;

CREATE TABLE etl_jobs (
  id int(11) DEFAULT NULL,
  name varchar(200) DEFAULT NULL,
  job_type varchar(20) DEFAULT NULL,
  is_enabled int(11) DEFAULT NULL,
  run_order int(11) DEFAULT NULL,
  last_run_date datetime DEFAULT NULL,
  last_run_status varchar(100) DEFAULT NULL,
  schedule varchar(20) DEFAULT NULL,
  last_success_date datetime DEFAULT NULL,
  force_run int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE etl_staging_defaults (
  id int(11) NOT NULL AUTO_INCREMENT,
  domain varchar(100) DEFAULT NULL,
  default_key varchar(100) DEFAULT NULL,
  value varchar(1000) DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=MyISAM AUTO_INCREMENT=100 DEFAULT CHARSET=utf8;

CREATE TABLE `etl_defaults` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `domain` varchar(100) CHARACTER SET latin1 DEFAULT NULL,
  `default_key` varchar(100) CHARACTER SET latin1 DEFAULT NULL,
  `value` varchar(1000) CHARACTER SET latin1 DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=101 DEFAULT CHARSET=utf8;

CREATE OR REPLACE VIEW v_etl_enabled_job_status
AS select
   t.job_type AS job_type,
   t.name AS name,timediff(ifnull(s.end_date,now()),s.start_date) AS duration,
   s.status AS status,
   s.phase AS phase,
   s.message AS message,cast(s.start_date as time) AS start_time, s.start_date AS start_date
from (etl_job_status s join etl_jobs t on((s.job_id = t.id)))
where (t.is_enabled = 1) order by s.start_date desc;

