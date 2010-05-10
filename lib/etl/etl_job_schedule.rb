class ETLJobSchedule
	include DataMapper::Resource

	property :id,          Serial
	property :job_name,    String
	property :argument,    String
	property :is_enabled,  Integer
	property :schedule,    String
	property :force_run,   Integer
	property :run_order,   Integer
end
