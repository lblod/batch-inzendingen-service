#!/usr/bin/env ruby
# coding: utf-8

STDOUT.sync = true

require 'linkeddata'
require 'date'
require 'securerandom'
require 'tempfile'
require 'csv'


class LoketDb
  attr_reader :client, :log
  SCHEMA= RDF::Vocab::SCHEMA
  ORG = RDF::Vocab::ORG
  FOAF = RDF::Vocab::FOAF
  SKOS = RDF::Vocab::SKOS
  DCTERMS = RDF::Vocab::DC
  PROV = RDF::Vocab::PROV
  RDFS = RDF::Vocab::RDFS
  MU = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/core/")
  PERSON = RDF::Vocabulary.new("http://www.w3.org/ns/person#")
  PERSOON = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/persoon#")
  MANDAAT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/mandaat#")
  BESLUIT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/besluit#")
  EXT = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/ext/")
  ADMS = RDF::Vocabulary.new('http://www.w3.org/ns/adms#')
  BASE_IRI='http://data.lblod.info/id'

  def initialize(endpoint)
    # @endpoint = endpoint
    # @client = SPARQL::Client.new(endpoint)
    @log = Logger.new(STDOUT)
    @log.level = Logger::INFO
    # wait_for_db
  end

  def write_ttl_to_file(name)
    output = Tempfile.new(name)
    begin
      output.write "# started #{name} at #{DateTime.now}"
      yield output
      output.write "# finished #{name} at #{DateTime.now}"
      output.close
      FileUtils.copy(output, File.join(ENV['OUTPUT_PATH'],"#{DateTime.now.strftime("%Y%m%d%H%M%S")}-#{name}.ttl"))
      output.unlink
    rescue StandardError => e
      puts e
      puts e.backtrace
      puts "failed to successfully write #{name}"
      output.close
      output.unlink
    end
  end
  def csv_parse_options
    { headers: :first_row, return_headers: true, encoding: 'iso-8859-1', col_sep: ';' }
  end

  def read_csv(file)
    headers_parsed = false
    index = 0
    begin
      ::CSV.foreach(file, csv_parse_options) do |row|
        unless headers_parsed
          @columnCount = row.size
          headers_parsed = true
          next
        end
        yield(index, row)
        index += 1
      end
    rescue ::CSV::MalformedCSVError => e
      log.error e.message
      log.error "parsing stopped after this error on index #{index}"
    end
  end
  def wait_for_db
    until is_database_up?
      log.info "Waiting for database... "
      sleep 2
    end

    log.info "Database is up"
  end
  def is_database_up?
    begin
      location = URI(@endpoint)
      response = Net::HTTP.get_response( location )
      return response.is_a? Net::HTTPSuccess
    rescue Errno::ECONNREFUSED
      return false
    end
  end

  def create_conversatie(nummer: , type:, betreft:, time:)
    uuid = SecureRandom.uuid
    conversatie = RDF::URI.new("http://data.lblod.info/id/conversaties/#{uuid}")
    graph = RDF::Repository.new
    graph << [ conversatie, RDF.type, SCHEMA.Conversation]
    graph << [ conversatie, MU.uuid, uuid]
    graph << [ conversatie, SCHEMA.identifier, nummer]
    graph << [ conversatie, SCHEMA.about, betreft]
    graph << [ conversatie, DCTERMS.type, type]
    graph << [ conversatie, SCHEMA.processingTime, time]
    [conversatie, graph]
  end

  def create_message(conversatie: nil , recipient: nil, dateReceived: nil, dateSent: nil, author: nil, sender: nil, isLastMessage: nil)
    uuid = SecureRandom.uuid
    message = RDF::URI.new("http://data.lblod.info/id/berichten/#{uuid}")
    graph = RDF::Repository.new
    graph << [ message, RDF.type, SCHEMA.Message]
    graph << [ message, MU.uuid, uuid]
    graph << [ message, SCHEMA.dateSent, dateSent] if dateSent
    graph << [ message, SCHEMA.dateReceived, dateReceived] if dateReceived
    graph << [ message, SCHEMA.recipient, recipient] if recipient
    graph << [ message, SCHEMA.sender, sender] if sender
    graph << [ conversatie, SCHEMA.author, author ] if author
    graph << [ conversatie, SCHEMA.hasPart, message] if conversatie
    graph << [ conversatie, EXT.lastMessage, message] if isLastMessage and conversatie
    [message, graph]
  end
end
