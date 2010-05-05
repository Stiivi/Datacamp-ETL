require 'test/unit/testsuite'
require 'basic_test'
require 'etl_manager_test'
require 'job_running_test'

class TS_DatacampETLTests

def self.suite
	suite = Test::Unit::TestSuite.new
 	suite << BasicTest.suite
	suite << ETLManagerTest.suite
	suite << JobRunningTest.suite
end

end
