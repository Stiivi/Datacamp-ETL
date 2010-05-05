require 'test/unit'
require 'etl'

class JobRunningTest < Test::Unit::TestCase
def setup
	@connection = Sequel.sqlite
	@manager = ETLManager.new(@connection)
	
	@manager.create_etl_manager_structures	
    @manager.job_search_path = ["jobs"]

	schedules = @connection[:etl_schedule]
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

def test_demand_run
	job = @manager.job_with_name('test')
	
	#assert_nothing_raised do
		job.run
	#end
end

def test_scheduled_run
	assert_nothing_raised do
		@manager.run_scheduled_jobs('daily')
	end
	
	table = @connection[:test_table]
	assert_equal(1, table.count)

	table = @connection[:etl_job_status]
	assert_equal(3, table.count)
	
end

end
