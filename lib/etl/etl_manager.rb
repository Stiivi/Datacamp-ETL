# ETL Manager 
#
# Copyright:: (C) 2010 Knowerce, s.r.o.
# 
# Author:: Stefan Urbanek
# Date:: Oct 2010
#

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

class ETLManager
attr_reader :connection

@connection_search_path = []

def initialize(connection)
	@connection = connection
	
	@job_search_path = Array.new
    self.log_file = STDERR
end

################################################################
# Initialization

def create_etl_manager_structures
    @connection.create_table(:etl_schedule) do
		primary_key :id
		string	    :name
		string	    :argument
		boolean     :is_enabled
		integer	    :run_order
		string	    :schedule
		integer		:force_run
	end

	@connection.create_table(:etl_defaults) do
		primary_key :id
		string 		:domain
		string 		:default_key
		string 		:value
	end

	@connection.create_table(:etl_job_status) do
		primary_key :id
		integer		:job_id
		string		:job_name
		string		:status
		string		:phase
		string		:message
		datetime	:start_time
		datetime	:end_time
	end
end

def log_file=(logfile)
    @log = Logger.new(logfile)
    @log.formatter = Logger::Formatter.new
    @log.datetime_format = '%Y-%m-%d %H:%M:%S '
    if @debug
        @log.level = Logger::DEBUG
    else
        @log.level = Logger::INFO
    end
end


################################################################
# Connections

def connection_search_path= (path)
	@connection_search_path = path
	reload_connections
end

def reload_connections
	@named_connection_infos = Hash.new
	
    @connection_search_path.each { |search_path|
    	path = Pathname.new(search_path)
		path.children.each { |file|
			begin
				hash = YAML.load_file(file)
				@named_connection_infos.merge!(hash)
			rescue
				@log.warn "Unable to include connections from file #{file}"		
			end
		}
    }
end

def named_connection_info(name)
	return @named_connection_infos[name.to_s]
end

def named_connection(name)
	if not @named_connections
		@named_connections = Hash.new
	end
	connection = @named_connections[name]
	if connection
		return connection
	end
	
	connection = Sequel.connect(named_connection_info.name)
	
end

################################################################
# Jobs

def scheduled_jobs(schedule)
	
	jobs = @connection[:etl_schedule]

	jobs = jobs.filter("is_enabled = 1 AND (force_run = 1 OR schedule = ?)", schedule)
	jobs = jobs.order(:run_order)

    return jobs.all
end

def enabled_jobs
	
	jobs = @connection[:etl_schedule]

	jobs = jobs.filter(is_enabled => 1)
	jobs = jobs.order(:run_order)

    return jobs.all
end

################################################################
# Job running

def run_scheduled_jobs(schedule)
	jobs = scheduled_jobs(schedule)

	@log.info "Running all scheduled jobs (#{jobs.count})"

    if jobs.nil? or jobs.empty?
        @log.info "No jobs to run"
	end	

	jobs.each { |job_info|
		run_named_job(job_info[:name], job_info[:argument])
	}
end

def run_named_job(name, argument = nil)

	# FIXME: reset force run flag
	@log.info "Running job #{name} (arg: '#{argument}')"

	bundle = JobBundle.bundle_with_name(name)
	
	if not bundle.is_loaded
		@log.info "Loading bundle for job #{name}"
		bundle.load
	end
	
	job = bundle.job_class.new(self, bundle)

	self.run_job(job, argument)
end

# FIXME: continue here
def run_job(job, argument)
	error = false

    @log.info "running job #{job.name} (argument: #{argument})"

    job_start_time = Time.now

	# FIXME: instantiate for each run (keep class not instance)

	# Prepare job status
	job.status = "running"
	job.start_time = Time.now
	create_job_status(job)
	
	# FIXME: Prepare defaults
	# job.defaults_domain = job_info.name if job.defaults_domain.nil?
	
	# job.defaults = ETLDefaults.new(job.defaults_domain)
	# job.last_run_date = job_info.last_run_date
	job.argument = argument
	job.prepare

	# FIXME: prefix log as job log
    if not @debug
        begin
            job.run
            job.finalize
        rescue
            job.status = "failed"
            job.message = $!.message
            job.finalize
        end
    else
        job.run
        job.finalize
    end    

	if job.status == "failed"
		@log.error "Job #{job.name} failed: #{job.message}"
	end

    job_elapsed_time = ((Time.now - job_start_time) * 100).round / 100

    @log.info "job #{job.name} finished. time: #{job_elapsed_time} s status:#{job.status}"
end

def create_job_status(job)
	# FIXME: write schedule ID
	status = {
			:job_name => job.name,
			:status => job.status,
			:phase => job.phase,
			:message => job.message,
			:start_time => job.start_time,
			:end_time => job.end_time
		}
	id = @connection[:etl_job_status].insert(status)
	job.status_id = id
end
def update_job_status(job)
	# FIXME: write schedule ID
	# FIXME: write sequential ID
	status = {
			:job_name => job.name,
			:status => job.status,
			:phase => job.phase,
			:message => job.message,
			:start_time => job.start_time,
			:end_time => job.end_time
		}
	rec = @connection[:etl_job_status].filter(:id => job.status_id)
	rec.update(status)
end

################################################################
# Defaults

def defaults_for_domain(domain)
	defaults = ETLDefaults.new(self, domain)
	return defaults
end

end
