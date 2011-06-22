require File.expand_path(File.dirname(__FILE__) + '/spec_helper')
require 'pp'

describe Mongo::Dequeue do
  
  def insert_and_inspect(body, options={})
  	 @queue.push(body,options)
     @queue.send(:collection).find_one  
  end
  
  
  before(:all) do
    opts   = {
      :database   => 'mongo_queue_spec',
      :collection => 'spec',
      :attempts   => 4,
      :timeout    => 60}
    @db = Mongo::Connection.new('localhost', nil, :pool_size => 4)
    @queue = Mongo::Dequeue.new(@db, opts)
  end
  
  before(:each) do
    @queue.flush!
  end
  
  describe "Configuration" do

    it "should set the connection" do
      @queue.connection.should be(@db)
    end

    it "should allow database option" do
      @queue.config[:database].should eql('mongo_queue_spec')
    end
    
    it "should allow collection option" do
      @queue.config[:collection].should eql('spec')
    end

    it "should allow attempts option" do
      @queue.config[:attempts].should eql(4)
    end
  
    it "should allow timeout option" do
      @queue.config[:timeout].should eql(60)
    end
  
    it "should have a sane set of defaults" do
      q = Mongo::Dequeue.new(nil)
      q.config[:collection].should eql 'mongo_queue'
      q.config[:attempts].should   eql 3
      q.config[:timeout].should    eql 300
    end
  end
  
  describe "Inserting a standard Job" do
    before(:each) do
       @item = insert_and_inspect({:message => 'MongoQueueSpec', :foo => 5})
    end
    
    it "should set priority to 3 by default" do
      @item['priority'].should be(3)
    end
    
    it "should not be complete" do
      @item['complete'].should be false
    end
    
    it "should have a null completed_at" do
    	@item['completed_at'].should be nil
    end
    
    it "should set a null locked_at" do
      @item['locked_at'].should be nil
    end
    
    it "should have no duplicates" do
      @item['count'].should be 1
    end
    
    it "should have a duplicate_key" do
      @item['duplicate_key'].should_not be nil 
    end
    
    it "should return struct body properly" do
      @item['body']['message'].should eql('MongoQueueSpec')
      @item['body']['foo'].should be(5)
    end
  end

	describe "Inserting different body types" do
	    before(:each) do
	       @queue.flush!
	    end
	    
	    it "should handle a struct" do
	    	i = insert_and_inspect({:message => 'MongoQueueSpec', :foo => 5})
		    i['body']['message'].should eql('MongoQueueSpec')
	     	i['body']['foo'].should be(5)
	    end

	    it "should handle a string" do
	    	i = insert_and_inspect("foobarbaz")
		    i['body'].should eql "foobarbaz"
	    end
	    
	    it "should handle a number" do
	    	i = insert_and_inspect(42)
		    i['body'].should be 42
	    end
	end

	describe "Deduplicating messages" do
	    before(:each) do
	       @queue.flush!
	    end
	    
	    it "should combine identical bodies" do
	    	a = insert_and_inspect("foo")
		    b = insert_and_inspect("foo")
		    @queue.send(:collection).count.should be 1  
			
	    end

	    it "should not combine different bodies" do
	    	a = insert_and_inspect("foo")
		    b = insert_and_inspect("bar")
		    @queue.send(:collection).count.should be 2
		    b['count'].should be 1
	    end
	end

	describe "Popping messages" do
	    before(:each) do
	       @queue.flush!
	    end
	    
	    it "should return message" do
	    	a = insert_and_inspect("foo")
		    m = @queue.pop
		    pp m
		    m['body'].should eq "foo"
		    @queue.send(:collection).count.should be 1  
			
	    end

	end

    
end