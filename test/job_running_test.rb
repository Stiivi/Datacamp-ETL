require 'test/unit'
require 'lib/job_manager'

class TestJobManager < Test::Unit::TestCase
def setup
    @manager = JobManager.new
    @configuration = YAML.load_file("config.yml")
    @manager.job_search_path = ["test/jobs"]
    @job_class = @manager.load_job_class("test", "extraction")
end

def test_raw_run
    @job = @job_class.new(@manager)
    assert_nothing_raised do
        @job.run
    end
end

end
