# Loading for Slovak public procurement
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

require 'lib/loading'

class VvoLoading < Loading
	def initialize(manager)
		super(manager)
    	@defaults_domain = 'vvo'
	end

	def run
		source_table = 'sta_procurements'
		dataset_table = 'ds_procurements'
		self.phase = 'init'

		mapping = create_identity_mapping(source_table)
		mapping[:batch_record_code] = :document_id
		
		execute_dataset_loading(source_table, dataset_table, mapping, :condition => :etl_loaded_date)
		finalize_dataset_loading(dataset_table)
		self.phase = 'end'
	end
	
end
