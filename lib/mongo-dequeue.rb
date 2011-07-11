require 'digest/md5'
require 'json'
require 'mongo'

# heavily inspired by https://github.com/skiz/mongo_queue

class Mongo::Dequeue
	attr_reader :collection, :config
	
	DEFAULT_CONFIG = {
		:timeout    => 300,
		:default_priority => 3
	}.freeze
	
	# Create a new instance of MongoDequeue with the provided mongodb connection and optional configuration.
	# See +DEFAULT_CONFIG+ for default configuration and possible configuration options.
	#
	# Example:
	#    db = Mongo::Connection.new('localhost')
	#    config = {:timeout => 90, :attempts => 2}
	#    queue = Mongo::Queue.new(db, config)
	#
	def initialize(collection, opts={})
		@collection = collection
		@config = DEFAULT_CONFIG.merge(opts)
	end
 
	# Remove all items from the queue. Use with caution!
	def flush!
		collection.drop
	end
	
	# Insert a new item into the queue.
	#
	# Example:
	#    queue.insert(:name => 'Billy', :email => 'billy@example.com', :message => 'Here is the thing you asked for')
	def push(body, item_opts = {})
		dup_key = item_opts[:duplicate_key] || Mongo::Dequeue.generate_duplicate_key(body)
		
		selector = {
			:duplicate_key => dup_key,
			:complete => false,
			:locked_at => nil
		}
		item = {
			'$set' => {
				:body => body,
				:inserted_at => Time.now.utc,
				:complete => false,
				:locked_till => nil,
				:completed_at => nil,
				:priority => item_opts[:priority] || @config[:default_priority],
				:duplicate_key => dup_key
			},
			'$inc' => {:count => 1 }
		}
		
		id = collection.update(selector, item, :upsert => true)
	end
	
	# Lock and return the next queue message if one is available. Returns nil if none are available. Be sure to
	# review the README.rdoc regarding proper usage of the locking process identifier (locked_by).
	# Example:
	#    doc = queue.pop()
	
	# {:body=>"foo", :id=>"4e039c372b70275e345206e4"}

	def pop(opts = {})
		begin
			timeout = opts[:timeout] || @config[:timeout]
			cmd = BSON::OrderedHash.new
		    cmd['findandmodify'] = collection.name
		    cmd['update']        = {'$set' => {:locked_till => Time.now.utc+timeout}} 
		    cmd['query']         = {:complete => false, '$or'=>[{:locked_till=> nil},{:locked_till=>{'$lt'=>Time.now.utc}}] }
		    cmd['sort']          = {:priority=>-1,:inserted_at=>1}
		    cmd['limit']         = 1
		    cmd['new']           = true
		    result = collection.db.command(cmd)
		rescue Mongo::OperationFailure => of
			return nil
		end
	    return {
	    	:body => result['value']['body'],
	    	:id => result['value']['_id'].to_s
	    }
	end
	
	# Remove the document from the queue. This should be called when the work is done and the document is no longer needed.
	# You must provide the process identifier that the document was locked with to complete it.
	def complete(id)
		begin
			cmd = BSON::OrderedHash.new
			cmd['findandmodify'] = collection.name
			cmd['query']         = {:_id => BSON::ObjectId.from_string(id), :complete => false}
			cmd['update']        = {'$set' => {:completed_at => Time.now.utc, :complete => true} }
			cmd['limit']         = 1
			collection.db.command(cmd)
		rescue Mongo::OperationFailure => of
			#opfailure happens when item has been already completed
			return nil
		end
	end
	
	# Removes completed job history
	def cleanup()
		collection.remove({:complete=>true});
	end
  	
	# Provides some information about what is in the queue. We are using an eval to ensure that a
	# lock is obtained during the execution of this query so that the results are not skewed.
	# please be aware that it will lock the database during the execution, so avoid using it too
	# often, even though it it very tiny and should be relatively fast.
	def stats
    	js = "function queue_stat(){
			      return db.eval(
			      function(){
			      	var nowutc = new Date();
			      	var a = db.#{collection.name}.count({'complete': false, '$or':[{'locked_till':null},{'locked_till':{'$lt':nowutc}}] });
			        var c = db.#{collection.name}.count({'complete': true});
			        var t = db.#{collection.name}.count();
			        var l = db.#{collection.name}.count({'complete': false, 'locked_till': {'$gte':nowutc} });
			        return [a, c, t, l];
			      }
			    );
			  }"
		available, complete, total, locked = collection.db.eval(js) 
		{ :locked    => locked.to_i,
		  :complete => complete.to_i,
		  :available => available.to_i,
		  :total     => total.to_i }
	end
	
	def self.generate_duplicate_key(body)
		return Digest::MD5.hexdigest(body) if body.class == "String"
		return Digest::MD5.hexdigest(body) if body.class == "Fixnum"
		#else	
		return Digest::MD5.hexdigest(body.to_json) #won't ever match a duplicate. Need a better way to handle hashes and arrays.
	end
	
	def peek
		firstfew = collection.find({
				:complete => false, 
				'$or'=>[{:locked_till=> nil},{:locked_till=>{'$lt'=>Time.now.utc}}]
			}, 
			:sort => [[:priority, :descending],[:inserted_at, :ascending]], 
			:limit => 10)
		return firstfew
	end
	
	
	protected
	
	def value_of(result) #:nodoc:
		result['okay'] == 0 ? nil : result['value']
	end
	
	
end