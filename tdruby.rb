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

querynumber= tablenumber.fetch_row
con.query("CREATE TABLE q#{querynumber[0]} (id int, nicetime datetime, event_type varchar(255)
,vector varchar(255),time int,solution varchar(255),timestamp int,badge_type varchar(255),badge_variant varchar(255),retailer_id int,badge_name varchar(255),product_id int,sku varchar(255),message_identifier bigint,session_id varchar(255),url varchar(255),content_hash varchar(255))")

job = cln.query('tracking', ARGV[0])
until job.finished?
  sleep 2
  job.update_progress!
  p job.update_progress!
end
rowcount = job.result.size
job.update_status!  # get latest info

while id<rowcount
  
  
  epochtime=job.result[id][1]
  data=job.result[id][0]
  sqlkeys=data.keys.join(",")
  sqlvalues= "'" << data.values.join("','") << "'"
  
  id=+1
  con.query("INSERT INTO q#{querynumber[0]}(id, nicetime, #{sqlkeys}) VALUES('#{id}','#{Time.at(epochtime)}',#{sqlvalues})")
  
  

end


