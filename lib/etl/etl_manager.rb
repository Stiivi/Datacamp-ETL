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
attr_reader :log
attr_reader :etl_files
attr_accessor :debug

cattr_reader :schedules_table_name
cattr_reader :defaults_table_name
cattr_reader :job_status_table_name
cattr_reader :system_table_names

@@schedules_table_name = :etl_schedules
@@defaults_table_name = :etl_defaults
@@job_status_table_name = :etl_job_status
@@system_table_names = 	[ @@schedules_table_name,
					   @@defaults_table_name,
					   @@job_status_table_name ]

@connection_search_path = []

def initialize(connection)
	@connection = connection
	@job_search_path = Array.new
    self.log_file = STDERR

	# check_etl_schema
end

def check_etl_schema
	@@system_tables.each {|table|
		if not @connection.table_exists?(table)
			raise RuntimeError, "ETL database schema is not initialized. Table #{table} is missing"
		end
	}

end
################################################################
# Initialization

def self.create_etl_manager_structures(connection, options = {})
	if options[:force] == true
		@@system_table_names.each { | table |
			if connection.table_exists?(table)
				connection.drop_table(table)
			end
		}
	else
		@@system_table_names.each { | table |
			if connection.table_exists?(table)
				raise RuntimeError, "Unable to create ETL structures. Table #{table} already exists."
			end
		}
	end

    connection.create_table(@@schedules_table_name) do
		primary_key :id
		String	    :name
		String	    :argument
		Integer     :is_enabled
		Integer	    :run_order
		String	    :schedule
		Integer		:force_run
	end

	connection.create_table(@@defaults_table_name) do
		primary_key :id
		String 		:domain
		String 		:default_key
		String 		:value
	end

	connection.create_table(@@job_status_table_name) do
		primary_key :id
		String		:job_name
		Integer		:schedule_id
		String		:status
		String		:phase
		String		:message
		DateTime	:start_time
		DateTime	:end_time
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

def all_schedules
	jobs = @connection[@@schedules_table_name]
	jobs = jobs.order(:run_order)
	return jobs
end

def planned_schedules(schedule = nil)

	if !schedule
		date = Date.today
		week_days = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]
		schedule = week_days[date.wday]
	end
	
	jobs = @connection[@@schedules_table_name]

	# Only daily/weekly schedules work at the moment
	jobs = jobs.filter(:is_enabled => 1)
	jobs = jobs.filter("(force_run = 1 OR schedule = ? OR schedule = 'daily')", schedule)
	jobs = jobs.order(:run_order)

    return jobs.all
end

def forced_schedules
	jobs = @connection[@@schedules_table_name]

	jobs = jobs.filter(:is_enabled => 1)
	jobs = jobs.filter(:force_run => 1)
	jobs = jobs.order(:run_order)

    return jobs.all
end

################################################################
# Job running

def run_scheduled_jobs
	jobs = planned_schedules
	@log.info "Running scheduled jobs (#{jobs.count})"
	run_schedules(jobs)
end

def run_forced_jobs
	jobs = forced_schedules
	@log.info "Running forced jobs (#{jobs.count})"
	run_schedules(jobs)
end

def run_schedules(schedules)
	if schedules.nil? or schedules.empty?
        @log.info "No schedules to run"
	end	

	schedules.each { |job_info|
		id = job_info[:id]
		name = job_info[:name]
		argument = job_info[:argument]
	    @log.info "Schedule #{job_info[:id]}: #{name}(#{argument})"
		run_named_job(name, argument)
	}
end

def run_named_job(name, argument = nil)

	# FIXME: reset force run flag
	bundle = JobBundle.bundle_with_name(name)
	@log.info "BUNDLE #{bundle.path}"
	if not bundle
		@log.error "Job #{name} does not exist"
		return
	end
	
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

    @log.info "Running job '#{job.name}' with argument '#{argument}'"

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

    @log.info "Job '#{job.name}' finished. time: #{job_elapsed_time}s status:#{job.status}"
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

# Other
def etl_files_path=(path)
    @etl_files_path = Pathname.new(path)
end


end
