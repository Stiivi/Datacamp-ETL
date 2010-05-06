# ETL - main ETL header
#
# Copyright:: (C) 2010 Knowerce, s.r.o.
# 
# Author:: Stefan Urbanek
# Date:: May 2010
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

require 'sequel'
require 'active_record'

require 'etl/class_additions'
require 'etl/job_bundle'
require 'etl/job'
require 'etl/etl_manager'
require 'etl/etl_defaults'
require 'etl/job_schedule'
require 'etl/connection_info'
# require 'etl/batch'
# require 'etl/download_batch'
# require 'etl/download_manager'
