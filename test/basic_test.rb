require 'test/unit'
require 'lib/job_manager'

class TestInstances < Test::Unit::TestCase
def test_instances
    @manager = JobManager.new
    assert_not_nil(@manager)
end
end
