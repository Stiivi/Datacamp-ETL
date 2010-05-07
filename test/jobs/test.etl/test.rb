class TestJob < Job
def run
	table = @connection[:test_table]
	if @argument == "fail"
		self.fail("test just failed")
	elsif @argument == "wait"
		log.info "Waiting for long operation to finish (sleeping)"
		self.phase = "waiting"
		sleep(10)
		self.phase = "finished"
		log.info "Long operation finished"
	else # == "fail"
		table.insert({:message => "test"})
	end
end
end
