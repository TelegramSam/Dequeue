require File.expand_path(File.dirname(__FILE__) + '/spec_helper')
require 'pp'

describe Mongo::Dequeue do
  
  def insert_and_inspect(body, options={})
  	 @queue.push(body,options)
     @queue.send(:collection).find_one  
  end
  
  
  before(:all) do
    opts   = {
      :timeout    => 60}
    @collection = Mongo::Connection.new('localhost', nil, :pool_size => 4).db('mongo_queue_spec').collection('spec')
    @queue = Mongo::Dequeue.new(@collection, opts)
  end
  
  before(:each) do
    @queue.flush!
  end
  
  describe "Configuration" do

    it "should set the connection" do
      @queue.collection.should be(@collection)
    end

    it "should allow timeout option" do
      @queue.config[:timeout].should eql(60)
    end
  
    it "should have a sane set of defaults" do
      q = Mongo::Dequeue.new(nil)
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
	    
	    it "should combine identical bodies of type string" do
	    	a = insert_and_inspect("foo")
		    b = insert_and_inspect("foo")
		    @queue.send(:collection).count.should be 1  
	    end

	    it "should not combine different bodies of type string" do
	    	a = insert_and_inspect("foo")
		    b = insert_and_inspect("bar")
		    @queue.send(:collection).count.should be 2
		    b['count'].should be 1
	    end
	    
	    it "should combine identical bodies of type struct" do
	    	pending "Test after we have a better way of handling structs"
	    	a = insert_and_inspect({:a=>'a',:b=>'b'})
		    b = insert_and_inspect({:a=>'a',:b=>'b'})
		    c = insert_and_inspect({:b=>'b',:a=>'a'})
		    @queue.send(:collection).count.should be 1
	    end

	    it "should not combine different bodies of type struct" do
	    	a = insert_and_inspect({:a=>'a',:b=>'b'})
		    b = insert_and_inspect({:a=>'a',:c=>'c'})
		    @queue.send(:collection).count.should be 2
		    b['count'].should be 1
	    end

	    it "should combine based on duplication_key" do
	    	a = insert_and_inspect({:a=>'a',:b=>'b'}, :duplicate_key => 'match')
		    b = insert_and_inspect({:a=>'a',:c=>'c'}, :duplicate_key => 'match')
		    @queue.send(:collection).count.should be 1
		    b['count'].should be 2
	    end
	    
	    it "should not combine based on duplication_key" do
	    	a = insert_and_inspect("foo", :duplicate_key => 'match')
		    b = insert_and_inspect("foo", :duplicate_key => 'nomatch')
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
		    m[:body].should eq "foo"
		    @queue.send(:collection).count.should be 1
	    end
	    
	    it "should return an id" do
	    	a = insert_and_inspect("foo")
		    m = @queue.pop
		    m[:id].should_not be nil
		    @queue.send(:collection).count.should be 1  
	    end

	    it "should return nil when queue is empty" do
	    	m = @queue.pop
		    m.should be nil
		    @queue.send(:collection).count.should be 0 
	    end
	    
	    it "should complete ok" do
	    	a = insert_and_inspect("foo")
		    m = @queue.pop
		    @queue.complete(m[:id])
		    m2 = @queue.pop
		    m2.should be nil
		    @queue.send(:collection).count.should be 1
	    end
	    
	    it "should pop again after timeout" do
	    	a = insert_and_inspect("foo")
		    m = @queue.pop(:timeout => 1)
		    sleep(2)
		    m2 = @queue.pop
		    m2[:id].should eq m[:id]
		    @queue.send(:collection).count.should be 1
	    end
	    
	    it "should pop in order" do
	       @a = insert_and_inspect("a")
	       @b = insert_and_inspect("b")
	       @c = insert_and_inspect("c")
	       @d = insert_and_inspect("d")
	       
	       @ap = @queue.pop
	       @bp = @queue.pop
	       @cp = @queue.pop
	       @dp = @queue.pop
	       
	       @ap[:body].should eq "a"
	       @bp[:body].should eq "b"
	       @cp[:body].should eq "c"
	       @dp[:body].should eq "d"
	    end
	    
	end
	
	describe "Peek" do
		it "should peek properly" do
			@a = insert_and_inspect("a")
			@b = insert_and_inspect("b")
			
			@peek = []
			p = @queue.peek
			p.each{|q| @peek << q }
			
			@peek.length.should == 2
		end
	end

	describe "Stats" do
	    before(:all) do
	       @queue.flush!
	       @a = insert_and_inspect("a")

	       @b = insert_and_inspect("b")
	       @c = insert_and_inspect("c")
	       @d = insert_and_inspect("d")
	       @e = insert_and_inspect("e")
	       
	       @ap = @queue.pop(:timeout => 1)
	       @bp = @queue.pop
	       @cp = @queue.pop
	       
	       sleep(2)
	       
	       @queue.complete(@bp[:id])
	       
	       @stats = @queue.stats
	    end
	    #locked, complete, available, total
	    it "should count complete" do
	    	@stats[:complete].should == 1
	    end
	    it "should count total" do
	    	@stats[:total].should == 5
	    end
	    it "should count available" do
	    	@stats[:available].should == 3
	    end
	    it "should count locked" do
	    	@stats[:locked].should == 1
	    end

	    
	end

    
end