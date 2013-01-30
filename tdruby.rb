require 'td'
require 'td-client'
require 'mysql'
require './settings.rb'




load_settings('tdrsettings.yaml')

cln = TreasureData::Client.new(@tdapikey)
con = Mysql.new @db_address, @db_user, @db_pass, @db_name

id = 0

epochtime = 0
tablenumber = con.query("SELECT count(*) from information_schema.tables WHERE table_schema = 'tdoutputs'")

querynumber = tablenumber.fetch_row

arg = ARGV[0]
columns = ARGV[0][/SELECT(.*?)FROM/,1].strip




if columns == "*"
  
  con.query("CREATE TABLE q#{querynumber[0]} (id int, query_id int, nicetime datetime, event_type varchar(255),vector varchar(255),time int,solution varchar(255),timestamp int,badge_type varchar(255),badge_variant varchar(255),retailer_id int,badge_name varchar(255),product_id int,sku varchar(255),published_review_count int, message_identifier bigint,session_id varchar(255),url varchar(255),content_hash varchar(255))")

  job = cln.query('tracking', ARGV[0])
  until job.finished?
    sleep 2
    job.update_progress!
    p job.update_progress!
  end
  job.update_status!  # get latest info

  job.result_each do |result|
    attributes, timestamp = result

    epochtime=timestamp
    fattributes=attributes.select {|k,v| ["event_type", "time", "vector", "solution", "timestamp", "badge_type", "badge_variant", "retailer_id", "sku", "badge_name", "product_id", "message_identifier", "session_id", "url", "content_hash","published_review_count"].include?(k) }
  
    sqlkeys=fattributes.keys.join(",")
    sqlvalues= "'" << fattributes.values.join("','") << "'"
  
    id=+1
    con.query("INSERT INTO q#{querynumber[0]}(id, query_id, nicetime, #{sqlkeys}) VALUES('#{id}','#{querynumber[0]}','#{Time.at(epochtime)}',#{sqlvalues})")
  
  
  

  end


  
  


else
  
  keys=columns.split(",")
  keyssql2=Array.new
  keyssql=keys.each do |a|
    
    
    keyssql2 << a.gsub(/[^0-9a-z ]/i, '')
    
    
  end
  
  
  stringsqlkeys=keyssql2.join(",").gsub(',', ' varchar(255),')
  stringsqlkeysdef = stringsqlkeys << " varchar(255)"
  

  
  con.query("CREATE TABLE q#{querynumber[0]} (id int,query_id int,#{stringsqlkeysdef})")
  
  job = cln.query('tracking', ARGV[0])
  until job.finished?
    sleep 2
    job.update_progress!
    p job.update_progress!
  end
  job.update_status!  # get latest info

  job.result_each do |result|
    sqlvalues=result.join(",")
    id=+1
    con.query("INSERT INTO q#{querynumber[0]}(id, query_id, nicetime, #{stringsqlkeys}) VALUES('#{id}','#{querynumber[0]}',#{sqlvalues})")
  
  end
  
end

#con.query("INSERT INTO query_index(id, query) VALUES('#{querynumber[0]}','#{ARGV[0]}')")