class TestJob < Job
def run
	table = @connection[:test_table]
	if @argument == "pass"
		table.insert({:message => "test"})
	else # == "fail"
		self.fail("test just failed")
	end
end
end
