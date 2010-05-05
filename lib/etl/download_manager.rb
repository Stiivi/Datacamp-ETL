# Download Manager
#
# Copyright (C) 2009 Stefan Urbanek
# 
# Author:: Stefan Urbanek
# Date:: November 2009
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

require 'monitor'
require 'pathname'
require 'etl/download_batch'

module DownloadManagerDelegate
def download_thread_did_start(download_manager, thread_id)
    # do nothing
end

def download_thread_did_finish(download_manager, thread_id)
    # do nothing
end

# Create a download batch. Batch should contain list of all URLs to be downloaded.
# Delegate should return _nil_ if no more files are to be downloaded.
# 
# Note: this method is called in thread-safe way.
def create_download_batch(download_manager, batch_id)
    return nil
end

def download_batch_failed(download_manager, batch_id)
end

def process_download_batch(download_manager, batch_id)
    # do nothing by default
end

def download_thread_failed(download_manager, thread_id, exception)
    puts "ERROR: Download thread #{thread_id} failed: #{exception.message}"
    puts exception.backtrace.join("\n")
end
end

class DownloadManager

attr_accessor :download_directory
attr_reader   :delegate
attr_accessor :thread_count

def initialize
    @thread_count = 1
    @download_directory = Pathname.new(".")
end

def delegate=(object)
    @delegate = object
end

def download
    # Prepare batch processing
    @processing_queue = Array.new
    @batch_id = 0

    # Prepare thread controll variables
    @mutex = Monitor.new
    @lock = @mutex.new_cond
    @download_finished = false

    # puts ">>> ROLLING THREADS"

    # Run the threads
    run_download_threads
    
    process_thread = Thread.new do
        process_downloads
    end
    
    # Wait for downloads to finish
    @download_threads.each { |thread| thread.join }
    @download_finished = true

    # puts "X== DOWNLOADS FINISHED (and not signaling anymore)"

    @mutex.synchronize do
        # puts "<-x LAST SIGNAL E:#{@processing_queue.empty?} F:#{@download_finished}"
        @lock.signal
    end
    # puts "-v- LAST JOIN"

    process_thread.join

    # puts "<<< DOWNLOAD END"

end

def process_downloads
    # Process downloads
    loop do
        batch = nil
        @mutex.synchronize do
            break if @download_finished and @processing_queue.empty?
            # puts "==> WAITING FOR SIGNAL"
            @lock.wait_while { @processing_queue.empty?} # or not @download_finished }
            # puts "<== GOT SIGNAL E:#{@processing_queue.empty?} F:#{@download_finished}"
            if not @processing_queue.empty?
                batch = @processing_queue.shift
            end
        end
        break unless batch
        @delegate.process_download_batch(self, batch)
    end
end

def stop_download
    @mutex.synchronize do
        @stop_download = true
    end
end

def run_download_threads
    @download_threads = Array.new

    for thread_id in 1..@thread_count
        @download_threads << Thread.new(thread_id) do 
            |tid|
            
            #FIXME: handle exception more intelligently
	        begin
                @delegate.download_thread_did_start(self, tid)
    
                loop do
                    # create new batch in thread-safe way
                    break if @stop_download
                    batch = create_next_batch
                    # puts "--- (#{tid}) GOT BATCH? #{not batch.nil?}"
                    break if not batch
    
                    if not download_batch(batch)
                        @delegate.download_batch_failed(self, batch)
                    end

                    # signalize that we are finished, so processing thread can
                    # start processing the downloaded batch
                    # queue batch even the download failed (wget fails when 
                    # last download fails)
                    @mutex.synchronize do
                         @processing_queue << batch
                         # puts "<-- SIGNAL"
                         @lock.broadcast
                    end
                end
                @delegate.download_thread_did_finish(self, tid)
            rescue => exception
                @delegate.download_thread_failed(self, tid, exception)
            end
            @mutex.synchronize do
                # puts "x-- SIGNAL END"
                @lock.broadcast
            end

        end
    end
end

def create_next_batch
    batch = nil
    @mutex.synchronize do
        @batch_id = @batch_id + 1
        batch = @delegate.create_download_batch(self, @batch_id)
        if not batch.nil?
            batch.id = @batch_id
        end
    end
    return batch
end
    
def download_batch(batch)
    # FIXME: create more download methods: ruby, curl, ...
    return download_batch_wget(batch)
end

# wget method of downloading
def download_batch_wget(batch)

    # FIXME: check permissions

    # create batch URL list file for wget
    list_path = @download_directory + "batch_url_list_#{batch.id}"

    file = File.open(list_path, "w")
    batch.urls.each { |url|
        file.puts url
    }
    file.close

    # create directory where files will be downloaded
    download_directory = @download_directory + "batch_files_#{batch.id}"
    
    download_directory.mkpath 

    result = system("wget", "-qE", "-P", download_directory, "-i", list_path)

    batch.files = download_directory.children.select { | path |
                      path.file?
                  }
    if !result
        return false
    end
    return true
end

end