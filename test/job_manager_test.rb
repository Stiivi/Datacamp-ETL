require 'test/unit'
require 'lib/job_manager'

class TestJobManager < Test::Unit::TestCase
def setup
    @manager = JobManager.new
    @configuration = YAML.load_file("config.yml")
    @manager.job_search_path = ["jobs", "test/jobs", "test/another_jobs_dir"]
end

def test_instantiate
    assert_not_nil(@manager)
end

def test_job_search_path
    assert_not_nil(@manager.path_for_job("test", "extraction"))
    assert_not_nil(@manager.path_for_job("test", "job"))
end

def test_job_load
    assert_not_nil(@manager.load_job_class("test", "extraction"), "unable load test extraction")
end

def test_wrong_job_superclass
    assert_raise RuntimeError do
        @manager.load_job_class("wrong_superclass", "extraction")
    end
end

def test_connection
    @manager.establish_connection(@configuration["connection"])
end
end
