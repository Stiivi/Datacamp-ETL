# TaskInfo - description of a task: name, schedule, priority,...
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

class TaskInfo < ActiveRecord::Base
	set_table_name "etl_tasks"

	def self.find_scheduled(task_type)
		task_type = task_type.to_s
		tasks = TaskInfo.find(:all, :conditions => ["is_enabled = 1 AND task_type = ?", task_type],
														:order => "run_order")
    	today = Time.now.beginning_of_day

        weekdays = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]

        tasks.select { | task |
            if (task.force_run == 1) 
                true
            elsif task.last_run_date.nil?
                if task.schedule == "daily" \
                        or task.schedule == "weekly" \
                        or weekdays.include?(task.schedule) and today.wday == weekdays.index(task.schedule)
                    true
                else
                    false
                end
            else
                last_run_day = task.last_run_date.beginning_of_day
               
                if (task.schedule == "daily" and last_run_day != today) \
                        or (task.schedule == "weekly" and (last_run_day - today) >= 7) \
                        or (weekdays.include?(task.schedule) and today.wday == weekdays.index(task.schedule) and last_run_day != today)
                    true
                else
                    false
                end
            end
        }
	end
	def self.find_enabled(task_type)
		task_type = task_type.to_s
		tasks = TaskInfo.find(:all, :conditions => ["is_enabled = 1 AND task_type = ?", task_type],
														:order => "run_order")
	end
end
