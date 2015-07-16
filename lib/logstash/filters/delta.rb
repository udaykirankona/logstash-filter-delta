# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require 'thread'
require 'socket'
require 'logger'
class LogStash::Filters::Delta < LogStash::Filters::Base

  TIMEOUT = 15 #timeout after 30 minutes
  PERIOD_FLUSH = 5
  config_name "delta"
  
  # start tag, read from config file
  config :start_tag, :validate => :string, :requried => true
  # end tag, read from config file
  config :end_tag, :validate => :string, :required => true
  # identifier tag
  config :id_tag, :validate => :string, :required => true

  config :periodic_flush, :validate => :boolean, :default => true
  

  public
  def register
    # Add instance variables 
    # register is called by logstash core per plugin instantiation
    @lock = Mutex.new
    # data store for start events
    @ht = {}
    @logger = LogStash::Logger.new
    @logger.info("timeout: #{TIMEOUT} seconds")
    puts "hello"
  end # def register

  public
  def filter(event)
    return unless filter?(event)
    
    if event[@id_tag].nil?
      return nil
    end

    if(isStartEvent?(event)) #start tag match
      # success
      filter_matched(event)
      puts "hello 2"
      @logger.info("delta, 'start event' received", start_tag: @start_tag, unique_id_field: @tag_id)      
      @lock.synchronize do
        #add this event into data store, if it does not exist already
        if !(@ht.has_key?(event[@id_tag]))
          @ht[event[@id_tag]] = Item.new(event)
        end
        # design choice ?? ignore event with start tag and existing id  or update with data store with new event
        # timestamp will defintelty get updated with option 2
        # option 1 used for now
      end
      

    elsif (isEndEvent?(event))
      # success
      filter_matched(event)
      @logger.info("delta, 'end event' received", end_tag: @end_tag, unique_id_field: @tag_id)
      @lock.synchronize do # has to be here, otherwise inconsistent state may arise
        if(@ht.has_key?(event[@id_tag]))   #if id already exists
          start_item=@ht[event[@id_tag]]  # delete start event from data store
          @ht.delete(event[@id_tag])       
          #take delta between start event and end event
          delta_t = event["@timestamp"] - start_item.event["@timestamp"]
          # generate a new event with delta_t
          new_event=create_new_event(delta_t,@id_tag)
          filter_matched(new_event)       # so that logstash can add fields
          yield new_event if block_given?
        else
          # no start event with id of end event
          # end event arrived but start event did not
          event.tag("unpaired end")
        end
      end #end lock synchronize
    end # end if-else
  end # def filter

  public
  def isStartEvent?(event)
    if(event["tags"]!=nil)
      return event["tags"].include?(@start_tag)
    end
  end

  public
  def isEndEvent?(event)
    if(event["tags"]!=nil)
      return event["tags"].include?(@end_tag)
    end
  end


  public
  def create_new_event(delta_t,id)
    new_event=LogStash::Event.new
    new_event["host"]=Socket.gethostname
    new_event.tag("delta_event")
    new_event.tag("delta_ok")
    new_event["delta_time"]=delta_t
    new_event["delta_id"]=id
    return new_event
  end

  public 
  def flush(options = {})
    puts "flush"
    @lock.synchronize do
      @ht.each do |key,item|  #increment all items by 5 ticks
        item.ticks = item.ticks + PERIOD_FLUSH
      end #end iterator
      @ht.each do |key,item|  #delete expired items
        if(item.ticks >= TIMEOUT)
          @ht.delete(key)
          puts "#{key}"
        else
          break #since iterator returns FIFO
        end
      end #end iterator
    end #end synchronize
    
  end
  
end # class LogStash::Filters::Example


class Item

  attr_accessor :event, :ticks
  
  def initialize(a)
    @event = a
    @ticks = 0
  end

end
