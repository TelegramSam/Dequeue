require 'digest/md5'
require 'json'
require 'mongo'

# heavily inspired by https://github.com/skiz/mongo_queue

class Mongo::Dequeue
	attr_reader :connection, :config
	
	DEFAULT_CONFIG = {
		:database   => 'mongo_queue',
		:collection => 'mongo_queue',
		:timeout    => 300,
		:attempts   => 3,
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
	def initialize(connection, opts={})
		@connection = connection
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
		dup_key = Mongo::Dequeue.generate_duplicate_key(body)
		
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
				:locked_at => nil,
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
	#    doc = queue.pop(Thread.current.object_id)

	def pop(opts = {})
		timeout = opts[:timeout] || @config[:timeout]
		cmd = OrderedHash.new
	    cmd['findandmodify'] = @config[:collection]
	    cmd['update']        = {'$set' => {:locked_till => Time.now.utc+timeout}} 
	    cmd['query']         = {:complete => false, '$or'=>[{:locked_till=> nil},{:locked_till=>{'$lt'=>Time.now.utc}}] }
	    cmd['sort']          = {:priority=>-1,:inserted_at=>-1}
	    cmd['limit']         = 1
	    cmd['new']           = true
	    value_of collection.db.command(cmd)
	end
	
	# Provides some information about what is in the queue. We are using an eval to ensure that a
	# lock is obtained during the execution of this query so that the results are not skewed.
	# please be aware that it will lock the database during the execution, so avoid using it too
	# often, even though it it very tiny and should be relatively fast.
	def stats
    	js = "function queue_stat(){
			      return db.eval(
			      function(){
			        var a = db.#{config[:collection]}.count({'locked_by': null, 'attempts': {$lt: #{config[:attempts]}}});
			        var l = db.#{config[:collection]}.count({'locked_by': /.*/});
			        var e = db.#{config[:collection]}.count({'attempts': {$gte: #{config[:attempts]}}});
			        var t = db.#{config[:collection]}.count();
			        return [a, l, e, t];
			      }
			    );
			  }"
		available, locked, errors, total = collection.db.eval(js)
		{ :locked    => locked.to_i,
		  :errors    => errors.to_i,
		  :available => available.to_i,
		  :total     => total.to_i }
	end
	
	def self.generate_duplicate_key(body)
		begin
			return Digest::MD5.hexdigest(JSON.generate(body))
		rescue
			return body
		end
	end
	
	
	protected
	
	def collection #:nodoc:
		@connection.db(@config[:database]).collection(@config[:collection])
	end
	
end