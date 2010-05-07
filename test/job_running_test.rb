require 'test/unit'
require 'etl'

class JobRunningTest < Test::Unit::TestCase
def setup
	@connection = Sequel.sqlite
	@manager = ETLManager.new(@connection)
	
	ETLManager.create_etl_manager_structures(@connection)
	JobBundle.job_search_path = ["jobs", "another_jobs_dir"]

	schedules = @connection[ETLManager.schedules_table_name]
	job = { :id => 1, :is_enabled => 1, :name => 'test', :argument => "pass", :schedule => 'daily' }
	schedules.insert(job)
	job = { :id => 2, :is_enabled => 1, :name => 'test', :argument => "fail", :schedule => 'daily' }
	schedules.insert(job)
	job = { :id => 3, :is_enabled => 1, :name => 'test', :argument => "fail", :schedule => 'daily' }
	schedules.insert(job)

	@connection.create_table :test_table do
		string :message
	end
end

def test_job_name
	bundle = JobBundle.bundle_with_name('test')
	job = bundle.job_class.new(@manager, bundle)
	assert_equal("test", job.name)
end

def test_single_run
	assert_nothing_raised do
		@manager.run_named_job('test')
	end
end

def test_scheduled_run
	assert_nothing_raised do
		@manager.run_scheduled_jobs
	end
	
	table = @connection[:test_table]
	assert_equal(1, table.count)

	table = @connection[:etl_job_status]
	assert_equal(3, table.count)
	
end

end
