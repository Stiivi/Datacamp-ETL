require 'test/unit'

require 'rubygems'
require 'etl'

class JobManagerTest < Test::Unit::TestCase
def test_pass
end
# def setup
#     @manager = JobManager.new
#     @configuration = YAML.load_file("config.yml")
#     @manager.job_search_path = ["jobs", "test/jobs", "test/another_jobs_dir"]
# end
# 
# def test_instantiate
#     assert_not_nil(@manager)
# end
# 
# def test_job_search_path
#     assert_not_nil(@manager.path_for_job("test", "extraction"))
#     assert_not_nil(@manager.path_for_job("test", "job"))
# end
# 
# def test_job_load
#     assert_not_nil(@manager.load_job_class("test", "extraction"), "unable load test extraction")
# end
# 
# def test_wrong_job_superclass
#     assert_raise RuntimeError do
#         @manager.load_job_class("wrong_superclass", "extraction")
#     end
# end
# 
# def test_connection
#     @manager.establish_connection(@configuration["connection"])
# end
# 
# def test_logger
#     log_file = Pathname.new('/tmp/datacap_etl_test.log')
#     if log_file.exist?
#         log_file.delete
#     end
#     @manager.log_file = log_file
#     assert_not_nil(@manager.logger)
#     @manager.logger.warn "Log test"
# 
#     @manager.debug = true
#     log_size = log_file.size
#     @manager.logger.debug "Debug log test"
#     assert_not_equal(log_file.size, log_size, "debug enabled, but debug message was not written into log")
# 
#     @manager.debug = false
#     log_size = log_file.size
#     @manager.logger.debug "Debug log test (should not be seen)"
#     assert_equal(log_file.size, log_size, "debug disabled, but debug message was written into log")
# 
# end

end
