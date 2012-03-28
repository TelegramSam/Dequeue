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
      q = Mongo::Dequeue.new(@collection)
      q.config[:timeout].should    eql 300
    end
  end

  describe "Inserting a standard Job" do
    before(:all) do
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

  describe "bulk inserting multiple jobs" do
    before(:all) do
      @queue.batchpush({:message => 'MongoQueueSpec1', :foo => 5})
      @queue.batchpush({:message => 'MongoQueueSpec2', :foo => 5})
      @queue.batchpush({:message => 'MongoQueueSpec3', :foo => 5})
    end

    it "should correctly count items in batch" do
      @queue.batch.length.should be(3)
    end

    it "should correctly add items on process" do
      @queue.batchprocess()
      @queue.send(:collection).count.should == 3
      @queue.batch.length.should == 0
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

    it "should unlock a queue item" do
      body = "Unlock Me"
      @queue.push body, {:priority => 1}
      item = @queue.pop
      @queue.unlock(item[:id])
      item2 = @queue.pop
      item2[:body].should eq body
    end

    it "should lock an item until a certain time" do
      body = "Lock me"
      @queue.push body, {:priority => 1}
      item = @queue.pop
      @queue.lock_until(item[:id], 5)
      Timecop.travel(Time.local(Time.now.year + 1)) do
        actual = @queue.pop
        actual[:body].should eq body
      end
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

    it "should consider the timeout value" do
      time    = Time.now
      timeout = 120
      item_id = nil

      Timecop.freeze time do
        @queue.push 'test item', {:timeout => timeout}
        item_id = @queue.pop[:id]
      end

      Timecop.freeze(time + 10) do
        @queue.pop.should eq nil
      end

      Timecop.freeze(time + timeout + 10) do
        actual = @queue.pop
        actual.should_not be_nil
        actual[:id].should eq item_id
      end
    end

    describe 'all the items have the same priority' do
      it "should be a standard FIFO queue" do
        items = %w(a b c d)

        items.each do |item|
          @queue.push item
        end

        items.each do |expected|
          actual = @queue.pop
          actual[:body].should eq expected
        end
      end
    end

    describe 'items have different priority' do
      it "should return items with higher priority first" do
        p3 = 'priority 3'
        p2 = 'priority 2'
        p1 = 'priority 1'

        @queue.push p1, {:priority => 1}
        @queue.push p3, {:priority => 3}
        @queue.push p2, {:priority => 2}

        @queue.pop[:body].should eq p3
        @queue.pop[:body].should eq p2
        @queue.pop[:body].should eq p1
      end

      it "should sort by priority and then by insertion" do
        p1       = 'priority 1'
        p3       = 'priority 3'
        p2_first = 'priority 2 - first insert'
        p2_last  = 'priority 2 - last insert'

        @queue.push p1, {:priority => 1}
        @queue.push p2_first, {:priority => 2}
        @queue.push p3, {:priority => 3}
        @queue.push p2_last, {:priority => 2}

        @queue.pop[:body].should eq p3
        @queue.pop[:body].should eq p2_first
        @queue.pop[:body].should eq p2_last
        @queue.pop[:body].should eq p1
      end

    end

  end

  describe "Peek" do
    it "should not remove items from the queue" do
      expected = %w(a b c d)

      expected.each do |item|
        @queue.push item
      end

      2.times do
        actual = @queue.peek.map{|i| i["body"]}
        actual.should eq expected
      end
    end

    it "should consider the timeout value" do
      time    = Time.now
      timeout = 120
      item_id = nil

      Timecop.freeze time do
        @queue.push 'test item', {:timeout => timeout}
        item_id = @queue.pop[:id]
      end

      Timecop.freeze(time + 10) do
        @queue.pop.should eq nil
        @queue.peek.count.should eq 0
      end

      Timecop.freeze(time + timeout + 10) do
        actual = @queue.peek.first
        actual.should_not be_nil
        actual['_id'].to_s.should eq item_id
      end

    end

    describe 'all the items have the same priority' do
      it "should act as a FIFO queue" do
        expected = %w(a b c d)

        expected.each do |item|
          @queue.push item
        end
        actual = @queue.peek.map{|i| i["body"]}
        actual.should eq expected
      end
    end

    describe 'items have different priority' do
     it "should return items with higher priority first" do
       p3       = 'priority 3'
       p2       = 'priority 2'
       p1       = 'priority 1'
       expected = [p3, p2, p1]

       @queue.push p1, {:priority => 1}
       @queue.push p3, {:priority => 3}
       @queue.push p2, {:priority => 2}

       actual = @queue.peek.map{|i| i["body"]}
       actual.should eq expected
     end

     it "should sort by priority and then by insertion" do
       p1       = 'priority 1'
       p3       = 'priority 3'
       p2_first = 'priority 2 - first insert'
       p2_last  = 'priority 2 - last insert'
       expected = [p3, p2_first, p2_last, p1]

       @queue.push p1, {:priority => 1}
       @queue.push p2_first, {:priority => 2}
       @queue.push p3, {:priority => 3}
       @queue.push p2_last, {:priority => 2}

       actual = @queue.peek.map{|i| i["body"]}
       actual.should eq expected
     end
    end

  end

  describe "Completing" do
    before(:all) do
      @a = insert_and_inspect("a")
      @b = insert_and_inspect("b")
      @c = insert_and_inspect("c")
      
      @ap = @queue.pop
      @queue.complete(@ap[:id])
      @ac = @queue.send(:collection).find_one({:_id => BSON::ObjectId.from_string(@ap[:id])})

      @bp = @queue.pop
      @bc = @queue.send(:collection).find_one({:_id => BSON::ObjectId.from_string(@bp[:id])})

      @cp = @queue.pop
      @queue.complete(@cp[:id])
      @queue.complete(@cp[:id])
      @queue.complete(@cp[:id])
      @cc = @queue.send(:collection).find_one({:_id => BSON::ObjectId.from_string(@cp[:id])})
      @stats = @queue.stats

    end
    it "should count a single completion" do
      @ac["completecount"].should eq 1

    end
    it "should report zero for uncompleted items" do
      @bc["completecount"].should eq 0
    end
    it "should count a single completion" do
      @cc["completecount"].should eq 3
    end
    it "should report stats correctly" do
      @stats[:redundantcompletes].should == 2
    end
  end

  describe "Stats" do
    before(:all) do
      @a = insert_and_inspect("a")

      @b = insert_and_inspect("b")
      @c = insert_and_inspect("c")
      @d = insert_and_inspect("d")
      @e = insert_and_inspect({:task => "foo"})

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
    it "should count redundant completes" do
      @stats[:redundantcompletes].should == 0
    end
    it "should count priorities" do
      #pp @stats[:priority]
      @stats[:priority].should == [{"priority"=>3.0, "complete"=>1.0, "waiting"=>4.0}]
    end
    it "should count tasks" do
      #pp @stats[:tasks]
      @stats[:tasks].should == [
        {"body.task"=>nil, "complete"=>1.0, "waiting"=>3.0},
        {"body.task"=>"foo", "complete"=>0.0, "waiting"=>1.0}
      ]
    end

  end

  describe "disable item timeout" do
    before(:each) do
      opts = {:timeout => nil}
      @queue = Mongo::Dequeue.new(@collection, opts)
      @queue.flush!
    end

    it "should work with pop" do
      expected = "hello world"
      @queue.push expected
      item = @queue.pop
      item[:body].should eq expected

      Timecop.travel(Time.local(Time.now.year + 10)) do
        actual = @queue.pop
        actual.should be_nil
      end
    end

    it "should work with peek" do
      expected = "hello world"
      @queue.push expected
      item = @queue.pop
      item[:body].should eq expected

      Timecop.travel(Time.local(Time.now.year + 10)) do
        actual = @queue.peek
        actual.count.should eq 0
      end
    end

  end

  describe "changing item priority" do
    describe "set a custom value" do
      it "work as expected" do
        expected_priority = 3
        item              = insert_and_inspect("Test", {:priority => 1})

        @queue.change_item_priority item['_id'], expected_priority

        item = @queue.send(:collection).find_one
        item["priority"].should eq expected_priority
      end

      it "should not complain if item is not found" do
        item = insert_and_inspect("Test", {:priority => 2})
        id = item['_id']

        @queue.send(:collection).remove('_id' => id)
        @queue.stats[:total].should eq 0

        lambda do
          @queue.change_item_priority(item['_id'], 1)
        end.should_not raise_error
      end

    end

    describe "increase the priority by steps" do
      it "should use 1 as default step value" do
        expected_priority = 3
        item              = insert_and_inspect("Test", {:priority => 2})

        @queue.increase_item_priority item['_id']

        item = @queue.send(:collection).find_one
        item["priority"].should eq expected_priority
      end

      it "should allow custom values for step" do
        expected_priority = 4
        item              = insert_and_inspect("Test", {:priority => 2})

        @queue.increase_item_priority item['_id'], 2

        item = @queue.send(:collection).find_one
        item["priority"].should eq expected_priority
      end

      it "should not complain if item is not found" do
        item = insert_and_inspect("Test", {:priority => 2})
        id = item['_id']

        @queue.send(:collection).remove('_id' => id)
        @queue.stats[:total].should eq 0

        lambda do
          @queue.increase_item_priority(item['_id'])
        end.should_not raise_error
      end

    end

    describe "decrease the priority by steps" do
      it "should use 1 as default step value" do
        expected_priority = 1
        item              = insert_and_inspect("Test", {:priority => 2})

        @queue.decrease_item_priority item['_id']

        item = @queue.send(:collection).find_one
        item["priority"].should eq expected_priority
      end

      it "should allow custom values for step" do
        expected_priority = 2
        item              = insert_and_inspect("Test", {:priority => 4})

        @queue.decrease_item_priority item['_id'], 2

        item = @queue.send(:collection).find_one
        item["priority"].should eq expected_priority
      end

      it "should not complain if item is not found" do
        item = insert_and_inspect("Test", {:priority => 2})
        id = item['_id']

        @queue.send(:collection).remove('_id' => id)
        @queue.stats[:total].should eq 0

        lambda do
          @queue.decrease_item_priority(item['_id'])
        end.should_not raise_error
      end

    end

  end

end
