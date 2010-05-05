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
attr_accessor :job_search_path
attr_reader :connection

def initialize(connection)
	@connection = connection
	
	@job_search_path = Array.new
    self.log_file = STDERR


#	ActiveRecord::Base.establish_connection(
#	  :adapter => "mysql",
#	  :host => @connection_info["host"],
#	  :username => @connection_info["username"],
#	  :password => @connection_info["password"],
#	  :database => @staging_schema,
#	  :encoding => 'utf8')

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
# Jobs

def path_for_job(job_name)
    @job_search_path.each { |search_path|
        job_path = Pathname.new("#{search_path}/#{job_name}.etl")
        return job_path if job_path.directory?        
    }
    return nil
end

def job_with_name(job_name)
	path = path_for_job(job_name)
	if not path
		raise RuntimeError, "Unknown job '#{job_name}'"
		return
	end
	
	info_file = Pathname.new(path) + 'info.yml'
	job_name = Pathname.new(path).basename.to_s.gsub(/\.[^.]*$/, "")
	
	if info_file.exist?
		# Info file exists
		
		info = YAML.load_file(info_file)
		if info[:job_type] == "ruby" or not info[:job_type]
			job = create_ruby_job(path, job_name, info)
		else
			raise RuntimeError, "Unknown job type"
		end
	else
		# No info file, assume ruby job
		job = create_ruby_job(path, job_name, nil)
	end
	return job

	# FIXME: Implem,ent this!
	#    @schema = @manager.staging_schema
	#    @table_prefix = "sta_"
	#    @config = @manager.domains_config[@defaults_domain.to_s]
	#    if not @config
	#        @config = Hash.new
	#    end
end

def create_ruby_job(path, name, info)
	###############################
	# Get executable file
	
	ruby_executable = nil
	if info
		ruby_executable = info["executable"]
	end
	
	if not ruby_executable
		ruby_executable = "#{name}.rb"
	end
	
	ruby_file = path + ruby_executable
	
    if not ruby_file.exist?
	    raise RuntimeError, "Unable to find ruby file #{ruby_file}"
	    return nil
    end
	
	require ruby_file
	
	class_name = nil
	if info
		class_name = info["class_name"]
	end
	if not class_name
		class_name = name.camelize + "ETLJob"
	end


	if not Class.class_exists?(class_name)
        raise RuntimeError, "Undefined class #{class_name} for job '#{name}'"
	end

    job_class = Class.class_with_name(class_name)

	@@job_superclass  = Job

	if not job_class.is_kind_of_class(@@job_superclass)
        raise RuntimeError, "Class #{job_class} is not kind of of #{@@job_superclass}"
    end
    
    job = job_class.new(self, name, info)
    
    return job
end

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

def run_scheduled_jobs(schedule)
	jobs = scheduled_jobs(schedule)

	@log.info "Running all scheduled jobs (#{jobs.count})"

    if jobs.nil? or jobs.empty?
        @log.info "No jobs to run"
	end	

	jobs.each { |job_info|
		job = job_with_name(job_info[:name])
		run_job(job, job_info[:argument])
	}
end

def run_job_(name, argument)

	# FIXME: reset force run flag
	@log.info "Running job #{name} (arg: '#{argument}')"

	job = job_with_name(name)

	job.run
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
	# FIXME: write sequential ID
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

end
