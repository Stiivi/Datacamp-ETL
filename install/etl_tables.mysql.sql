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
) ENGINE=InnoDB AUTO_INCREMENT=49 DEFAULT CHARSET=utf8;

CREATE TABLE etl_staging_defaults (
  id int(11) NOT NULL AUTO_INCREMENT,
  domain varchar(100) DEFAULT NULL,
  default_key varchar(100) DEFAULT NULL,
  value varchar(1000) DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=MyISAM AUTO_INCREMENT=48 DEFAULT CHARSET=utf8;

CREATE TABLE etl_task_status (
  id int(11) NOT NULL AUTO_INCREMENT,
  task_name varchar(255) DEFAULT NULL,
  task_id int(11) DEFAULT NULL,
  status varchar(255) DEFAULT NULL,
  phase varchar(255) DEFAULT NULL,
  message varchar(255) DEFAULT NULL,
  start_date datetime DEFAULT NULL,
  end_date datetime DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=MyISAM AUTO_INCREMENT=601 DEFAULT CHARSET=utf8;

CREATE TABLE etl_tasks (
  id int(11) DEFAULT NULL,
  name varchar(200) DEFAULT NULL,
  task_type varchar(20) DEFAULT NULL,
  is_enabled int(11) DEFAULT NULL,
  run_order int(11) DEFAULT NULL,
  last_run_date datetime DEFAULT NULL,
  last_run_status varchar(100) DEFAULT NULL,
  schedule varchar(20) DEFAULT NULL,
  last_success_date datetime DEFAULT NULL,
  force_run int(11) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE OR REPLACE VIEW v_etl_task_status
AS select
   t.task_type AS task_type,
   t.name AS name,timediff(ifnull(s.end_date,now()),s.start_date) AS duration,
   s.status AS status,
   s.phase AS phase,
   s.message AS message,cast(s.start_date as time) AS start_time
from (etl_task_status s join etl_tasks t on((s.task_id = t.id)))
where (t.is_enabled = 1) order by s.start_date desc;