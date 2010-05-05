require 'test/unit/testsuite'
# require 'job_manager_test'
require 'etl_manager_test'
require 'job_running_test'

class TS_DatacampETLTests

def self.suite
	suite = Test::Unit::TestSuite.new
# 	suite << JobManagerTest.suite
	suite << ETLManagerTest.suite
end

end
