require 'test/unit'

require 'rubygems'
require 'etl'

class ETLManagerTest < Test::Unit::TestCase
def setup
	@manager = ETLManager.new('sqlite3::memory:')
	@manager.create_etl_manager_structures(:force => true)
	@manager.debug = true
	
	JobBundle.job_search_path = ["jobs", "another_jobs_dir"]
	RepositoryManager.default_manager.search_path = ["repositories"]
end

def test_connections
	repo_manager = RepositoryManager.default_manager
	
	info = repo_manager.repository("shop")
	assert_not_nil(info)
	
	connection = Sequel.connect('sqlite:/')
	repo_manager.add_named_connection(connection, "default")
	connection2 = repo_manager.named_connection("default")
	assert_equal(connection, connection2)
	
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
	assert_equal(6, @manager.all_schedules.count)

	# daily + mon+force
	assert_equal(2, @manager.planned_schedules("daily").count)

	# mon, mon, daily
	assert_equal(3, @manager.planned_schedules("monday").count)

	# sat, daily, mon+force
	assert_equal(3, @manager.planned_schedules("saturday").count)

	# two forced, but one is not enabled
	assert_equal(1, @manager.forced_schedules.count)
end

def schedule_some_jobs
	schedule = ETLJobSchedule.new({ :id => 1, :is_enabled => 1, :job_name => 'daily', :schedule => 'daily' })
	schedule.save

	schedule = ETLJobSchedule.new({ :id => 2, :is_enabled => 1, :job_name => 'mon_job', :schedule => 'monday' })
	schedule.save

	schedule = ETLJobSchedule.new({ :id => 3, :is_enabled => 1, :job_name => 'sat_job', :schedule => 'saturday' })
	schedule.save

	schedule = ETLJobSchedule.new({ :id => 4, :is_enabled => 1, :job_name => 'forced', :schedule => 'monday', :force_run => 1 })
	schedule.save

	schedule = ETLJobSchedule.new({ :id => 5, :is_enabled => 0, :job_name => 'forced', :schedule => 'monday', :force_run => 1 })
	schedule.save

	schedule = ETLJobSchedule.new({ :id => 6, :is_enabled => 0, :job_name => 'forced', :schedule => 'daily'})
	schedule.save
end

end
