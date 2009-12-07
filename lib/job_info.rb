# JobInfo - description of a job: name, schedule, priority,...
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

class JobInfo < ActiveRecord::Base
set_table_name "etl_jobs"

def self.find_enabled(options = nil)
    job_type = nil
    scheduled = false

    if options
        job_type = options[:job_type]
        job_type = job_type.to_s if job_type
        scheduled = options[:scheduled]
    end            
        
    if job_type
        jobs = JobInfo.find(:all, :conditions => ["is_enabled = 1 AND job_type = ?", job_type],
                                                        :order => "run_order")
    else
        jobs = JobInfo.find(:all, :conditions => ["is_enabled = 1"],
                                                        :order => "run_order")
    end
    
    today = Time.now.beginning_of_day

    weekdays = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]

    if scheduled
        jobs = jobs.select { | job |
            if (job.force_run == 1) 
                true
            elsif job.last_run_date.nil?
                if job.schedule == "daily" \
                        or job.schedule == "weekly" \
                        or (weekdays.include?(job.schedule) and today.wday == weekdays.index(job.schedule))
                    true
                else
                    false
                end
            else
                last_run_day = job.last_run_date.beginning_of_day
               
                if (job.schedule == "daily" and last_run_day != today) \
                        or (job.schedule == "weekly" and (last_run_day - today) >= 7) \
                        or ((weekdays.include?(job.schedule) and today.wday == weekdays.index(job.schedule) and last_run_day != today))
                    true
                else
                    false
                end
            end
        }
    end

    return jobs
end

end
