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

require 'lib/job'
require 'lib/batch'

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

	def create_dataset_mapping_sql(source_table, dataset_table, map)
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

	def sql_value(value)

		# FIXME: this is mysql specific

		if value.nil? then
			string_value = "NULL"
		elsif value.class == String or value.class == Symbol
			# FIXME: handle symbol as .to_s?
			string_value = "#{value}"
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
	
	def execute_dataset_loading(source_table, dataset_table, mapping, options = nil)
		self.phase = 'mapping'
		statement = create_dataset_mapping_sql(source_table, dataset_table, mapping)		

        options = {} if options.nil?

		# FIXME: make this more intelligent

		# if not @last_run_date.nil?
		#	date_string = @last_run_date.strftime('%Y%m%d%H%M%S')
		#	condition = "date_created > STR_TO_DATE('#{date_string}', '%Y%m%d%H%i%S')"
		# end
        if options[:condition] == :etl_loaded_date
            condition = "etl_loaded_date IS NULL"
        end
        
        if condition 
			statement = "#{statement} WHERE #{condition}"
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
end
