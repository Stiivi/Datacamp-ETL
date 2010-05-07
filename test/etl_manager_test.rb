require 'test/unit'

require 'rubygems'
require 'etl'

class ETLManagerTest < Test::Unit::TestCase
def setup
	@connection = Sequel.sqlite
	@manager = ETLManager.new(@connection)
	
	ETLManager.create_etl_manager_structures(@connection)

	JobBundle.job_search_path = ["jobs", "another_jobs_dir"]
	@manager.connection_search_path = ["connections"]
end

def test_initialize
	ETLManager.system_table_names.each { |table|
		check_table(table)
	}
end

def test_connections
	connection = @manager.named_connection_info("shop")
	assert_not_nil(connection)
end

def check_table(table_name)
	assert_nothing_raised do
		table = @connection[table_name]
		table.count
	end
end

def test_job_search_path
    assert_not_nil(JobBundle.path_for_job("test2"))
    assert_not_nil(JobBundle.path_for_job("test"))
end

def test_job_bundle
	job = JobBundle.bundle_with_name("test")
	assert_not_nil(job)
	assert_equal("test", job.name)
end
def test_no_info_job_bundle
	job = JobBundle.bundle_with_name("no_info")
	assert_not_nil(job)
	assert_equal("no_info", job.name)
end

def test_no_info_job_bundle
	assert_raise RuntimeError do
		bundle = JobBundle.bundle_with_name("wrong_superclass")
		job_class = bundle.job_class
	end
end

def test_schedules
	jobs = @manager.planned_schedules("daily")
	assert_equal(0, jobs.count)

	schedule_some_jobs
	schedules = @connection[ETLManager.schedules_table_name]
	assert_equal(6, schedules.count)

	jobs = @manager.planned_schedules("daily")
	# daily + mon+force
	assert_equal(2, jobs.count)

	jobs = @manager.planned_schedules("monday")
	# mon, mon, daily
	assert_equal(3, jobs.count)

	jobs = @manager.planned_schedules("saturday")
	# sat, daily, mon+force
	assert_equal(3, jobs.count)
end

def schedule_some_jobs
	schedules = @connection[ETLManager.schedules_table_name]
	job = { :id => 1, :is_enabled => 1, :name => 'daily', :schedule => 'daily' }
	schedules.insert(job)
	job = { :id => 2, :is_enabled => 1, :name => 'mon_job', :schedule => 'monday' }
	schedules.insert(job)
	job = { :id => 3, :is_enabled => 1, :name => 'sat_job', :schedule => 'saturday' }
	schedules.insert(job)
	job = { :id => 4, :is_enabled => 1, :name => 'forced', :schedule => 'monday', :force_run => 1 }
	schedules.insert(job)
	job = { :id => 5, :is_enabled => 0, :name => 'forced', :schedule => 'monday', :force_run => 1 }
	schedules.insert(job)
	job = { :id => 6, :is_enabled => 0, :name => 'forced', :schedule => 'daily'}
	schedules.insert(job)
end

end
