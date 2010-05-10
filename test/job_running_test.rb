require 'test/unit'
require 'etl'

class JobRunningTest < Test::Unit::TestCase
def setup
	@manager = ETLManager.new('sqlite3::memory:')
	@manager.create_etl_manager_structures(:force => true)
	@manager.debug = true

	JobBundle.job_search_path = ["jobs", "another_jobs_dir"]
	@connection_manager = ConnectionManager.default_manager
	@connection_manager.connection_search_path = ["connections"]
		
	schedule = ETLJobSchedule.new({ :is_enabled => 1, :job_name => 'test', :argument => 'pass', :schedule => 'daily' })
	schedule.save
	schedule = ETLJobSchedule.new({ :is_enabled => 1, :job_name => 'test', :argument => 'fail', :schedule => 'daily' })
	schedule.save
	schedule = ETLJobSchedule.new({ :is_enabled => 1, :job_name => 'test', :argument => 'fail', :schedule => 'daily' })
	schedule.save
	
	@connection = Sequel.sqlite
	@connection_manager.add_named_connection(@connection, "default")

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
	table = @connection[:test_table]
	
	table.delete
	assert_equal(0, table.count)

	assert_nothing_raised do
		@manager.run_scheduled_jobs
	end
		
	table = @connection[:test_table]
	assert_equal(1, table.count)
end

end
