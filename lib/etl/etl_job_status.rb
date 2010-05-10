class ETLJobStatus
	include DataMapper::Resource

	property :id,          Serial
	property :job_name,    String
	property :status,      String
	property :phase,       String
	property :message,     String
	property :start_time,   DateTime
	property :end_time,     DateTime
end
