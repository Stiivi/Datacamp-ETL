require 'lib/task'
require 'csv'

class DatastoreDump < Task
@@metadata_columns = [:quality_status, :updated_at, :batch_id, 
                        :created_at, :validity_date, :created_by, :is_hidden,
                        :updated_by, :record_status, :batch_record_code]

def initialize(manager)
    super(manager)
end

def run
    # do nothing
    @table_prefix = @defaults.value(:dataset_table_prefix, "ds_")

    # FIXME: this is mysql specific
    Sequel::MySQL.convert_invalid_date_time = true

    tables = dataset_tables
    
    count = 0
    tables.each { | table |
        dump_table(table)
        count = count + 1
    }
    
    @defaults[:tables_dumped] = count
end

def dataset_tables
    expr = "SELECT table_name 
                FROM information_schema.tables
                WHERE table_schema = '#{@manager.dataset_schema}'
                    AND table_name LIKE '#{@table_prefix}%'"
    data = @connection[expr]
    
    data.collect { | row | row[:table_name] }
end
def columns_for_table(table)
    expr = "SELECT column_name 
                FROM information_schema.columns
                WHERE table_schema = '#{@manager.dataset_schema}'
                    AND table_name = '#{table}'"
    data = @connection[expr]
    
    data.collect { | row | row[:column_name].to_sym }
end

def dump_table(table)
    if @manager.configuration["dataset_dump_path"]
        path = Pathname(@manager.configuration["dataset_dump_path"])
    else
        path = Pathname(".")
    end

    dump_file = path + "#{table}-dump.csv"
    # FIXME: remove prefix 

    columns = columns_for_table(table)

    table_sym = "#{@manager.dataset_schema}__#{table}".to_sym
    data = @connection[table_sym]

    self.phase = "table #{table}"

    CSV.open(dump_file, 'w') do |csv|
        csv << columns

        data.each {  |row|
            csv << columns.collect { |col| row[col] }
        }

    end
end

end
