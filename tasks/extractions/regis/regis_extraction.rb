# RegisExtraction - extraction of register of organisations in Slovakia
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

require 'lib/extraction'
require 'monitor'
require 'iconv'
require 'hpricot'
require 'rexml/document'

class RegisExtraction < Extraction
include MonitorMixin
    
def initialize(manager)
    super(manager)
    
    @defaults_domain = 'regis'
    @target_table = "#{@manager.staging_schema}__sta_regis_main".to_sym
end

def setup_defaults
    @defaults[:data_source_name] = "Regis"
    @defaults[:data_source_url] = "http://www.statistics.sk/"
    @download_url_base = "http://www.statistics.sk/pls/wregis/detail?wxidorg="
    @defaults[:download_url] = @download_url_base

    # last id: 1054548 (as of 2009-10)
    @download_start_id = defaults.value(:download_start_id, 1).to_i
    @download_last_id = defaults.value(:download_last_id, 1000000).to_i
    
    @batch_size = defaults.value(:batch_size, 200).to_i
    @download_threads = defaults.value(:download_threads, 10).to_i
    @download_fail_threshold = defaults.value(:download_fail_threshold, 10).to_i

    @defaults[:files_directory] = files_directory

    @file_encoding = "iso-8859-2"
end

def setup_paths
    path = Pathname.new(files_directory)
    
    @downloads_path = path + 'downloads'
    @downloads_path.mkpath
end

def run
    setup_defaults
    setup_paths
    
    self.phase = "init"

    files_to_download = @download_last_id - @download_start_id
    files_per_thread = files_to_download / @download_threads

    clean_up
    
    #######################################################
    # Prepare downloads

    self.phase = "launching threads"

    self.logger.info "threads:#{@download_threads}"
    self.logger.info "download id from #{@download_start_id} to #{@download_last_id}"

    @batches_pending_lock = self.new_cond
    @batches_to_process = []
    @download_finished = false

    thread_start_id = 0
    @batch_id = 0
    
    download_threads = []
 
    #######################################################
    # Downloads
    
    for thread_id in 1..@download_threads
        thread_end_id = [thread_start_id + files_per_thread, @download_last_id].min

        self.logger.info "running download thread #{thread_id} range: #{thread_start_id}-#{thread_end_id}"
        
        download_threads << Thread.new(thread_id, thread_start_id, thread_end_id) do 
            |tid, tstart_id, tend_id|

            batch_id = 0
            start_id = tstart_id
            while start_id < tend_id
                end_id = [start_id + @batch_size - 1, tend_id].min
                
                self.logger.info "thread #{tid}: downloading batch #{batch_id} range:#{start_id}-#{end_id}"

                url_list = create_urls(start_id, end_id)
                create_thread_batch_from_list(tid, batch_id, url_list)
                download_batch(tid, batch_id)

                # signalize that we are finished, so processing thread can
                # start processing the downloaded batch
                
                self.synchronize do
                    @batches_to_process << [tid, batch_id]
                    @batches_pending_lock.signal
                end

                start_id = end_id + 1
                batch_id = batch_id + 1
            end
        end
        
        thread_start_id = thread_start_id + files_per_thread + 1
    end
    
    #######################################################
    # Process
    
    self.logger.info "running processing thread"

    process_thread = Thread.new do 
        loop do
            batch = nil
            self.synchronize do
                break if @download_finished and @batches_to_process.empty?
                @batches_pending_lock.wait_while { @batches_to_process.empty? }
                batch = @batches_to_process.shift
            end
            break unless batch
            thread_id = batch[0]
            batch_id = batch[1]

            self.logger.info "processing thread #{thread_id} batch #{batch_id} (#{@batches_to_process.count} remaining)"
            process_thread_batch(thread_id, batch_id)
        end
        self.logger.info "processing finished"
    end

    #######################################################
    # Wait

    self.logger.info "waiting for downloads to finish"
    self.phase = "waiting for downloads"

    download_threads.each { |thread| thread.join }

    self.logger.info "downloads finished"
    @download_finished = true

    self.logger.info "waiting for batch process to finish"
    self.phase = "waiting for batch process"
    process_thread.join
    
    #######################################################
    # Finalize

    self.logger.info "finalizing"
    self.phase = "finalizing"

    table = connection[@target_table].filter("etl_loaded_date IS NULL")
    max_doc_id = table.max(:doc_id)

    failed_pages = @download_last_id - max_doc_id

    @defaults[:status_failed_pages] = failed_pages
    @defaults[:status_last_downloaded_id] = max_doc_id

    self.logger.info "last id:#{max_doc_id} fail count:#{failed_pages}"

    if failed_pages < @download_fail_threshold
        @defaults[:download_last_id] = @download_last_id + @download_fail_threshold
        self.logger.info "increasing last id to #{@defaults[:download_last_id]}"
    else
        self.logger.info "seems to be at end, keeping last download id"
    end
end

def clean_up
    self.phase = "cleanup"

    self.logger.info "removing downloaded files in #{@downloads_path}"

    @downloads_path.children.each { |path|
        if path.directory?
            path.rmtree
        else
            path.delete
        end
    }
    
end

def create_urls(id_from, id_to)
    list = []
    id_from.upto(id_to) { |i|
        list << "#{@download_url_base}#{i}"
    }
    return list
end

def create_thread_batch_from_list(thread_id, batch_id, url_list)
    path = batch_list_path(thread_id, batch_id)

    file = File.open(path, "w")
    url_list.each { |url|
        file.puts url
    }
    file.close
end

def download_batch(thread_id, batch_id)
    batch_file = batch_list_path(thread_id, batch_id)
    batch_path = batch_downloads_path(thread_id, batch_id)
    batch_path.mkpath 
    # FIXME: handle this more gracefuly and between threads
    if not system("wget", "-Eq", "-P", batch_path, "-i", batch_file)
        self.logger.error "unable to run wget"
    end
    
    self.logger.info "thread #{thread_id}: batch #{batch_id} download finished"
end

def process_thread_batch(thread_id, batch_id)
    batch_path = batch_downloads_path(thread_id, batch_id)
    batch_files = batch_path.children.select { | path |
                              path.extname == ".html"
                          }

    batch_files.each { |file|
        process_file(file)
    }
end


def batch_downloads_path(thread_id, batch_id)
    return @downloads_path + "batch_#{batch_id}_#{thread_id}"
end

def batch_list_path(thread_id, batch_id)
    return files_directory + "batch_#{batch_id}_#{thread_id}_url_list"
end

def id_from_filename(filename)
    docid = filename.to_s.gsub(/(.*=)([0-9]+)(\.html)$/,'\2')
    return docid.to_i
end

def process_file(file_name)
    # process_file_h2(file_name)
    # process_file_hpricot2(file_name)
    process_file_hpricot(file_name)
end
def process_file_hpricot(file_name)
    # FIXME: handle process exception: issue warning, return and do not delete file

    file = File.open(file_name)

    if File.size?(file).nil?
        self.logger.info "empty file #{file.basename}"
        return
    end
    
    doc_id = id_from_filename(file_name)

    contents = file.read
    contents = Iconv.conv("utf-8", @file_encoding, contents)
    document = Hpricot contents
    xml = Hpricot::XML contents
    # 7 9 13
    body = xml.children[2].children[1].children[3]
    tables = body.children[3].children[3].children[5]
    table1 = tables.children[1]
    table2 = tables.children[1].children[7].children[1]
    table3 = tables.children[1].children[9].children[1]
    table4 = tables.children[1].children[13].children[1]
    
    ico = table1.children[1].children[5].children[0].children[0].to_s
    name = table1.children[3].children[5].children[0].children[0].to_s
    legal_form = table1.children[5].children[5].children[0].children[0].to_s
    legal_form = legal_form.to_s.gsub(/ \-.*/,'')
    
    date_start = table2.children[1].children[5].children[0].children[0].to_s
    date_end = table2.children[3].children[5].children[0]

    if date_end.is_a?(Hpricot::Elem)
        date_end = date_end.children[0].to_s
    else
        date_end = date_end.to_s
    end

    address = table3.children[1].children[5].children[0].children[0].to_s
    region = table3.children[3].children[5].children[0].children[0].to_s

    activity1 = table4.children[5].children[3].children
    activity1 = activity1[0].to_s if not activity1.nil?

    activity2 = table4.children[7].children[3].children
    activity2 = activity2[0].to_s if not activity2.nil?
    
    account_sector = table4.children[9].children[3].children
    account_sector = account_sector[0].to_s if not account_sector.nil?

    ownership = table4.children[11].children[3].children
    ownership = ownership[0].to_s if not ownership.nil?

    size = table4.children[13].children[3].children
    size = size[0].to_s if not size.nil?

    date_start_splits = date_start.split('.')
    date_start = date_start_splits.reverse.join('-')
    if date_end != '-'
        date_end_splits = date_start.split('.')
        date_end = date_end_splits.reverse.join('-')
    end
    region.strip!
    address.gsub!('?', '')
    
    date_end = nil if date_end == '-'
    size = nil if size == "  "
    
    url = @download_url_base.to_s + doc_id.to_s
    
    connection[@target_table].insert(
            :doc_id => doc_id,
            :ico => ico,
            :name => name,
            :legal_form => legal_form,
            :date_start => date_start,
            :date_end => date_end,
            :address => address,
            :region => region,
            :activity1 => activity1,
            :activity2 => activity2,
            :account_sector => account_sector,
            :ownership => ownership,
            :size => size,
            :date_created => Time.now,
            :source_url => url)
    # delete processed file
    file_name.delete
end
end
