# Loading job
#
# Copyright (C) 2009 Knowerce, s.r.o.
# 
# Written by: Stefan Urbanek
# Date: Oct 2009
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

require 'etl/job'
require 'etl/batch'

class Loading < Job
attr_reader :output_tables, :temporary_tables, :enumeration_tables
attr_reader :data_source_url
attr_reader :data_source_name

def prepare
    @batch = Batch.new
    @batch.batch_type = 'loading'
    @batch.batch_source = self.name
    if @data_source_name
        @batch.data_source_name = @data_source_name
    else
        @batch.data_source_name = @defaults.value('data_source_name', nil)
    end
    if @data_source_url
        @batch.data_source_url = @data_source_url
    else
        @batch.data_source_url = @defaults.value('data_source_url', nil)
    end
    @batch.batch_date = Time.now
    @batch.username = 'system_loading'
    @batch.save
end

def name
    self.class.name.gsub(/Loading$/,'').downcase
end

def create_identity_mapping(source_table)
    columns = column_names_for_table(@manager.staging_schema, source_table)
    map = Hash.new
    columns.each { | column | 
        if not staging_system_columns.include?(column)
            map[column.to_sym] = column.to_sym
        end
    }

    return map
end

def column_names_for_table(schema, table)
    # FIXME: database specific
    statement = "SELECT column_name
                    FROM information_schema.columns 
                    WHERE table_schema = '#{schema}' AND table_name = '#{table}'"
                    
    columns = @connection[statement]

    return columns.collect { |column| column[:column_name].to_sym }
end

def create_dataset_append_sql(source_table, dataset_table, map)
    statements 	  = Array.new
    target_fields = Array.new
    values 		  = Array.new

    map.each { |field, value|
        string_value = sql_value(value)

        target_fields.push field
        values.push string_value
        
        statements.push "#{string_value} AS #{field}"
    }
    target_fields.push "record_status"
    values.push        "'loaded'"
    
    target_string = target_fields.join(",")
    values_string = values.join(",")
    statement = "INSERT INTO #{@manager.dataset_schema}.#{dataset_table}
                                (#{target_string})
                            SELECT #{values_string}
                                FROM #{@manager.staging_schema}.#{source_table}"

    return statement;
end	

def sql_value(value, options = nil)

    # FIXME: this is mysql specific
    # FIXME: does not work when you need table prefix
    
    if options
        table = options[:table]
    end

    if value.nil? then
        string_value = "NULL"
    elsif value.class == String or value.class == Symbol
        # FIXME: handle symbol as .to_s?
        if table
            string_value = "#{table}.#{value}"
        else
            string_value = "#{value}"
        end
    elsif value.class == Hash
        if value[:type] == :string  then
            string_value = "'#{value[:value]}'"
        elsif value[:type] == :sql or value[:type] == :number then
            string_value = "#{value[:value]}"
            # FIXME: handle undefined cases, raise an exception
        elsif value[:type] == :date then
            string_value = "STR_TO_DATE(#{value[:value]},'%d.%m.%Y')"
        end
    # FIXME: handle undefined cases, raise an exception
    end
    
    string_value
end

def append_table_with_map(source_table, dataset_table, mapping, options = nil)
    self.phase = 'mapping'
    statement = create_dataset_append_sql(source_table, dataset_table, mapping)		

    options = {} if options.nil?

    # FIXME: make this more intelligent

    # if not @last_run_date.nil?
    #	date_string = @last_run_date.strftime('%Y%m%d%H%M%S')
    #	condition = "date_created > STR_TO_DATE('#{date_string}', '%Y%m%d%H%i%S')"
    # end
    if options[:condition]
        condition = options[:condition] 
    end
    
    if condition 
        statement = "#{statement} WHERE #{condition}"
    end
    execute_sql(statement)

    if options[:condition] == :etl_loaded_date
        set_loaded_flag(source_table)
    end
end

def update_table_with_map(source_table, dataset_table, mapping, key, options = nil)
    self.phase = 'mapping'

    sets = Array.new

    mapping.each { |field, value|
        string_value = sql_value(value, :table => "sta")

        sets.push "ds.#{field} = #{string_value}"
    }

    sets.push "record_status = 'loaded'"
    
    set_mapping = sets.join(",")

    statement = "UPDATE #{@manager.dataset_schema}.#{dataset_table} ds,
                        #{@manager.staging_schema}.#{source_table} sta
                    SET #{set_mapping}
                    WHERE ds.#{key} = sta.#{key}"

    options = {} if options.nil?

    if options[:condition]
        condition = options[:condition] 
    end
    
    if condition 
        condition = condition.gsub(/@TABLE/, "sta")
        statement = "#{statement} AND (#{condition})"
    end

    execute_sql(statement)

    if options[:condition] == :etl_loaded_date
        set_loaded_flag(source_table)
    end
end

def set_loaded_flag(source_table)
    @connection << "UPDATE #{@manager.staging_schema}.#{source_table}
                            SET etl_loaded_date = NOW()"
end

def finalize_dataset_loading(dataset_table)
    self.phase = 'finalize'
    # FIXME: create batch record /production

    # FIXME: this is mysql specific /database
    today = "NOW()"
    batch_id = @batch.id
    set_expression = "created_at = #{today},
                      created_by = 'system_loading',
                      quality_status = 'ok',
                      batch_id = #{batch_id},
                      validity_date = NULL,
                      is_hidden = true"
                      
    statement = "UPDATE #{@manager.dataset_schema}.#{dataset_table}
                    SET #{set_expression}, record_status = 'new'
                    WHERE record_status = 'loaded'"
    execute_sql(statement)
end

def create_table_diff(diff_table, schema1, table1, schema2, table2, key_field, fields)
        md5sum1 = "tmp_#{table1}_md5"
        md5sum2 = "tmp_#{table2}_md5"

        create_record_md5_table(md5sum1, schema1, table1, key_field, fields)
        create_record_md5_table(md5sum2, schema2, table2, key_field, fields)

        drop_staging_table(diff_table)
        
        news_sql = "CREATE TABLE #{@manager.staging_schema}.#{diff_table} AS
                    SELECT t1.#{key_field} #{key_field}, 'n' diff
                    FROM #{@manager.staging_schema}.#{md5sum1} t1
                    LEFT JOIN #{@manager.staging_schema}.#{md5sum2} t2 ON t1.ico = t2.ico
                    WHERE t2.ico IS NULL"

        self.logger.info "diff: adding news"

        execute_sql(news_sql)
        create_staging_table_index(diff_table, key_field.to_s)

        diffs_sql = "INSERT INTO #{@manager.staging_schema}.#{diff_table} (#{key_field}, diff)
                    SELECT t1.#{key_field} #{key_field}, 'c' diff
                    FROM #{@manager.staging_schema}.#{md5sum1} t1
                    JOIN #{@manager.staging_schema}.#{md5sum2} t2 ON t1.ico = t2.ico
                    WHERE t2.md5_sum != t1.md5_sum"

        self.logger.info "diff: adding changes"
        execute_sql(diffs_sql)

end

def create_record_md5_table(target_table, table_schema, source_table, key_field, fields)

    # FIXME: MySQL specific! use NVL
    
    fields = fields.collect {|field| "COALESCE(#{field}, '')"}
    joined_fields = fields.join(",")
    
    drop_staging_table(target_table)

    sql = "CREATE TABLE #{@manager.staging_schema}.#{target_table}
                  AS SELECT #{key_field.to_s}, MD5(CONCAT(#{joined_fields})) md5_sum
                     FROM #{table_schema}.#{source_table}"

    execute_sql(sql)
    
    create_staging_table_index(target_table, key_field)
end

def drop_staging_table(table)
    execute_sql("DROP TABLE IF EXISTS #{@manager.staging_schema}.#{table.to_s}")
end

def create_staging_table_index(target_table, key_field)
    @connection.add_index staging_table_symbol(target_table), key_field.to_sym
end

def staging_table_symbol(table)
    return "#{@manager.staging_schema}__#{table.to_s}".to_sym
end

end
