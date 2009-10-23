# Extraction of public procurement
#
# Copyright (C) 2009 Aliancia Fair Play
# 
# Written by: Michal Barla
# Date: August 2009
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

require 'tasks/extractions/vvo/models/procurement'
require 'hpricot'
require 'iconv'
require 'pathname'

class VvoExtraction < Extraction
  FIRST_PROCUREMENT_ID = 2641
  DOWNLOAD_OFFSET = 5
  
  def initialize(manager)
    super(manager)
    @base_url = 'http://www.e-vestnik.sk/EVestnik/Detail/'
    @defaults_domain = 'vvo'
  end

  def run
	@defaults[:data_source_name] = "E-Vestnik"
	@defaults[:data_source_url] = "http://www.e-vestnik.sk/"
    @download_dir = files_directory + Time.now.strftime("%y%m%d%H%M%S")
    @download_dir.mkpath
    @download_from = defaults.value(:download_interval_from, FIRST_PROCUREMENT_ID).to_i
    download_procurements @download_from, @download_from + DOWNLOAD_OFFSET
    process_procurements
  end
  
  private
  def process_procurements
    d = Dir.new(@download_dir)
    d.each {|file|
      next if(file == "." || file == "..")

      file_content = File.open(@download_dir.to_s + "/#{file}").read
      file_content = file_content.gsub("&nbsp;",' ')

      encoded_file_content = Iconv.conv("utf-8", "windows-1250", file_content)
      document_id = file.split('.').first

      doc = Hpricot(encoded_file_content)

      checked_value = (doc/"//tr[2]/td[@class='typOzn']")
      if(checked_value.nil?)
        #puts "FAILURE: Did not find announcement type, omitting file: #{file}"
        next
      else
        if(checked_value.inner_text.match(/Oznámenie o výsledku verejného obstarávania/))
          store(parse(doc),document_id)
        else
          #puts checked_value.inner_text
          #puts "#{file} is not result announcement"
          if((doc/"//div[@id='innerMain']/div/text()").inner_text == "Oznámenie nebolo nájdené")
              defaults[:download_interval_from] = document_id
              break
          end
      end
    end
  }
  if(defaults[:download_interval_from].to_i == @download_from)
    defaults[:download_interval_from] = defaults[:download_interval_from].to_i + DOWNLOAD_OFFSET
  end
end
  
  def download_procurements(from_doc_id, to_doc_id)
    for document_id in from_doc_id..to_doc_id
      #print "Downloading #{document_id}..."
      download(procurement_url(document_id), "#{@download_dir}/#{document_id}.html")
      #puts "done."
    end
  end
  
  def download(url, file)
    if not system("wget", "-T", "5", "-x", "-O", "#{file}", "#{url}")
       raise "Unable to run wget"
    end
  end
  
def parse(doc)
  procurement_id = (doc/"//div[@id='innerMain']/div/h2").inner_text
  bulletin_and_year_content = (doc/"//div[@id='innerMain']/div/div").inner_text

  md = bulletin_and_year_content.gsub(/ /,'').match(/Vestníkč.(\d*)\/(\d*)/u)
  bulletin_id = md[1] unless md.nil?
  year = md[2] unless md.nil?

  customer_ico_content = (doc/"//table[@class='mainTable']/tbody/tr[7]/td/table/tbody/tr[2]/td[2]/table/tbody/tr[2]/td/").inner_text

  #we want to be sure, that we selected ICO with the XPath
  md = customer_ico_content.gsub(/ /,'').match(/IČO:(\d*)/u)
  unless (md.nil?)
    customer_ico = md[1]
  else
    customer_name = (doc/"//table[@class='mainTable']/tbody/tr[7]/td/table/tbody/tr[2]/td[2]/table/tbody/tr[1]/td/").inner_text	
    puts "unable to find ico for #{customer_name}"
    #we should try regis here, but it seems that 2009 procurements are all ok
  end

  procurement_subject = (doc/"//table[@class='mainTable']/tbody/tr[9]/td/table/tbody//span[@class='hodnota']").first.inner_text

  supplier_content = (doc/"//table[@class='mainTable']/tbody/tr[13]").inner_text

  #there could be multiple suppliers, ich supplying part of the procurement with separate price
  md_supp_arr = supplier_content.downcase.gsub(/ /,'').scan(/názovaadresadodávateľa,sktorýmsauzatvorilazmluva\s*^.*$\s*iČo:(\d*)/u)

  md_price_arr_from_supp_content = supplier_content.downcase.gsub(/ /,'').scan(/(celkovákonečnáhodnotazákazky:\s*hodnota|hodnota\/najnižšiaponuka\(ktorásabraladoúvahy\)):(\d*[,|.]?\d*)(\w*)\s*(bezdph|sdph|vrátanedph)*/u)

  suppliers = Array.new

  for i in 0..md_supp_arr.size-1
    supplier_ico = "#{md_supp_arr[i][0]}"
    unless(md_price_arr_from_supp_content[i].nil?) #if we were able to match price here
      price = md_price_arr_from_supp_content[i][1]
      currency = md_price_arr_from_supp_content[i][2]
      vat_included = true
      vat_included = false if md_price_arr_from_supp_content[i][3] == "bezdph"
    end
  suppliers << {:supplier_ico => supplier_ico, :price => price, :currency => currency, :vat_included => vat_included}
  end

  record = { :customer_ico => customer_ico, 
             :suppliers => suppliers,
             :procurement_subject => procurement_subject,
             :year => year,
             :bulletin_id => bulletin_id,
             :procurement_id => procurement_id
            }
  return record
end

def store(procurement, document_id)

  procurement[:suppliers].each do |supplier|
    Procurement.create({
        :document_id => document_id,
        :year => procurement[:year],
        :bulletin_id => procurement[:bulletin_id],
        :procurement_id => procurement[:procurement_id],
        :customer_ico => procurement[:customer_ico],
        :supplier_ico => supplier[:supplier_ico],
        :procurement_subject => procurement[:procurement_subject],
        :price => supplier[:price],
        :currency => supplier[:currency],
        :is_VAT_included => supplier[:vat_included],
        :customer_ico_evidence => "",
        :supplier_ico_evidence => "",
        :subject_evidence => "",
        :price_evidence => "",
        :source_url => procurement_url(document_id),
        :date_created => Time.now})
  end

end 

def procurement_url(document_id)
  return "#{@base_url}#{document_id}"
end
  
end
