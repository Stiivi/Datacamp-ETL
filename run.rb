#! /usr/bin/ruby
#
require 'lib/task_manager'

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

task_manager = TaskManager.new
task_manager.logger = logger

begin
    task_manager.establish_connection(configuration["connection"])
    task_manager.staging_schema = configuration["staging_schema"]
    task_manager.dataset_schema = configuration["dataset_schema"]
    
    if configuration["tasks_path"]
        task_manager.tasks_path = configuration["tasks_path"]
    else
        task_manager.tasks_path = Pathname.new(__FILE__).dirname + "tasks"
    end
    
    if configuration["etl_files_path"]
        task_manager.etl_files_path = configuration["etl_files_path"]
    end
    
    
    task_manager.configuration = configuration
    
    task_manager.run_scheduled_tasks_of_type("extraction")
    task_manager.run_enabled_tasks_of_type("loading")
    task_manager.run_enabled_tasks_of_type("dump")
    logger.info "Finished"
rescue => exception
    logger.error "EXCEPTION: #{exception.message}"
    logger.error exception.backtrace.join("\n")
end

exit
