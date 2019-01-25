require 'fileutils'
require_relative 'loket_db'

DC = RDF::Vocab::DC
NFO = RDF::Vocabulary.new('http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#')
NIE = RDF::Vocabulary.new('http://www.semanticdesktop.org/ontologies/2007/01/19/nie#')
DBPEDIA = RDF::Vocabulary.new('http://dbpedia.org/ontology/')
FILE_SERVICE_RESOURCE_BASE = 'http://mu.semte.ch/services/batch-inzendingen/'

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
    date =  Date.strptime(row['Datum verzending'], "%d/%m/%Y")
    (message, data) = loket.create_message(conversatie: conv, recipient: RDF::URI.new(row['BestuurseenheidURI'].strip), sender: RDF::URI.new('http://data.lblod.info/id/bestuurseenheden/141d9d6b-54af-4d17-b313-8d1c30bc3f5b') , dateReceived: date, dateSent: date, isLastMessage: true )
    update(%(
      INSERT DATA INTO <#{graph}> {
          #{data.dump(:ntriples)}
      }
    ))
  end

  rewrite_url = rewrite_url_header(request)
  error('X-Rewrite-URL header is missing.') if rewrite_url.nil?
  error('File parameter is required.') if params['file'].nil?

  tempfile = params['file'][:tempfile]

  upload_resource_uuid = generate_uuid()
  upload_resource_name = params['file'][:filename]
  upload_resource_uri = "#{FILE_SERVICE_RESOURCE_BASE}/files/#{upload_resource_uuid}"

  file_format = 'text/csv'
  file_extension = upload_resource_name.split('.').last
  file_size = File.size(tempfile.path)

  file_resource_uuid = generate_uuid()
  file_resource_name = "#{file_resource_uuid}.#{file_extension}"
  file_resource_uri = "share://batch-inzending-#{file_resource_uuid}"

  now = DateTime.now

  query =  " INSERT DATA {"
  query += "   GRAPH <#{graph}> {"
  query += "     <#{upload_resource_uri}> a <#{NFO.FileDataObject}> ;"
  query += "         <#{NFO.fileName}> #{upload_resource_name.sparql_escape} ;"
  query += "         <#{MU_CORE.uuid}> #{upload_resource_uuid.sparql_escape} ;"
  query += "         <#{DC.format}> #{file_format.sparql_escape} ;"
  query += "         <#{NFO.fileSize}> #{sparql_escape_int(file_size)} ;"
  query += "         <#{DBPEDIA.fileExtension}> #{file_extension.sparql_escape} ;"
  query += "         <#{DC.created}> #{now.sparql_escape} ;"
  query += "         <#{DC.modified}> #{now.sparql_escape} ."
  query += "     <#{file_resource_uri}> a <#{NFO.FileDataObject}> ;"
  query += "         <#{NIE.dataSource}> <#{upload_resource_uri}> ;"
  query += "         <#{NFO.fileName}> #{file_resource_name.sparql_escape} ;"
  query += "         <#{MU_CORE.uuid}> #{file_resource_uuid.sparql_escape} ;"
  query += "         <#{DC.format}> #{file_format.sparql_escape} ;"
  query += "         <#{NFO.fileSize}> #{sparql_escape_int(file_size)} ;"
  query += "         <#{DBPEDIA.fileExtension}> #{file_extension.sparql_escape} ;"
  query += "         <#{DC.created}> #{now.sparql_escape} ;"
  query += "         <#{DC.modified}> #{now.sparql_escape} ."
  query += "   }"
  query += " }"
  update(query)

  content_type 'application/vnd.api+json'
  status 201
  {
    data: {
      type: 'files',
      id: upload_resource_uuid,
      attributes: {
        name: upload_resource_name,
        format: file_format,
        size: file_size,
        extension: file_extension
      }
    },
    links: {
      self: "#{rewrite_url.chomp '/'}/#{upload_resource_uuid}"
    }
  }.to_json
end
