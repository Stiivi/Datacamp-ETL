#! /usr/bin/ruby
#
require 'lib/job_manager'

if ARGV[0].nil?
	config_file = "config.yml"
else
	config_file = ARGV[0]
end

configuration = YAML.load_file(config_file)

if configuration["log_file"]
    log_file = configuration["log_file"]
else
    log_file = STDERR
end

logger = Logger.new(log_file)
logger.formatter = Logger::Formatter.new
#################################################################
# Run scheduled extractions

job_manager = JobManager.new
job_manager.logger = logger

begin
    job_manager.establish_connection(configuration["connection"])
    job_manager.staging_schema = configuration["staging_schema"]
    job_manager.dataset_schema = configuration["dataset_schema"]
    
    if configuration["job_search_path"]
        job_manager.job_search_path = configuration["job_search_path"]
    else
        job_manager.job_search_path = [Pathname.new(__FILE__).dirname + "jobs"]
    end
    
    if configuration["etl_files_path"]
        job_manager.etl_files_path = configuration["etl_files_path"]
    end
    
    
    job_manager.configuration = configuration
    
    job_manager.run_scheduled_jobs_of_type("extraction")
    job_manager.run_enabled_jobs_of_type("loading")
    job_manager.run_enabled_jobs_of_type("dump")
    logger.info "Finished"
rescue => exception
    logger.error "EXCEPTION: #{exception.message}"
    logger.error exception.backtrace.join("\n")
end

exit
