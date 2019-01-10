require 'fileutils'
require_relative 'loket_db'

post '/conversatie-upload' do
  tempfile = params['file'][:tempfile]
  loket = LoketDb.new(ENV['MU_SPARQL_ENDPOINT'])
  loket.read_csv(tempfile.path) do |index , row|
    (conv, data) = loket.create_conversatie(nummer: row['Dossiernummer'], type: row['Type communicatie'], betreft: row['Betreft'], time: "P31D")
    update(%(
      INSERT DATA INTO <#{graph}> {
          #{data.dump(:ntriples)}
      }
    ))
    date =  Date.strptime(row['Datum verzending'], "%m/%d/%Y")
    (message, data) = loket.create_message(conversatie: conv, recipient: RDF::URI.new(row['BestuurseenheidURI'].strip), sender: RDF::URI.new('http://data.lblod.info/id/bestuurseenheden/141d9d6b-54af-4d17-b313-8d1c30bc3f5b') , dateReceived: date, dateSent: date, isLastMessage: true )
    update(%(
      INSERT DATA INTO <#{graph}> {
          #{data.dump(:ntriples)}
      }
    ))
  end
  content_type 'application/vnd.api+json'
  status 202
end

