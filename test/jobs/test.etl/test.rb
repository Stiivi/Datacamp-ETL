class TestJob < Job
def prepare
	connection_manager = ConnectionManager.default_manager
	@connection = connection_manager.named_connection("default")
	if !@connection
		@connection = connection_manager.create_connection("default", "default")	
	end
end

def run
	if @argument == "fail"
		self.fail("This test was planned to fail")
	elsif @argument == "wait"
		log.info "Waiting for long operation to finish (sleeping)"
		self.phase = "waiting"
		sleep(10)
		self.phase = "finished"
		log.info "Long operation finished"
	else # == "fail"
		log.info "Test Job - insert"
		table = @connection[:test_table]
		table.insert({:message => "test"})
	end
end
end
