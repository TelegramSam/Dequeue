require 'digest/md5'
require 'json'
require 'mongo'

# heavily inspired by https://github.com/skiz/mongo_queue

class Mongo::Dequeue
	attr_reader :collection, :config, :batch

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
		@batch = []
	end

	# Remove all items from the queue. Use with caution!
	def flush!
		collection.drop
	end

	# Insert a new item into the queue.
  # Valid options:
  #   - :priority: integer value, 3 by default.
  #   - :duplicate_key
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
        :locked => false,
				:locked_till => nil,
				:completed_at => nil,
				:priority => item_opts[:priority] || @config[:default_priority],
				:duplicate_key => dup_key,
				:completecount => 0
			},
			'$inc' => {:count => 1 }
		}

		id = collection.update(selector, item, :upsert => true)
	end

	# add a new item into the delayed batch
	def batchpush(body, item_opts = {})
		@batch << {
			:body => body,
			:duplicate_key => item_opts[:duplicate_key] || Mongo::Dequeue.generate_duplicate_key(body),
			:priority => item_opts[:priority] || @config[:default_priority]
		}
	end

	def batchprocess()
		js = %Q|
    		function(batch) {
    			var nowutc = new Date();
    			var ret = [];
    			for(i in batch){
    				e = batch[i];
    				//ret.push(e);
    				var query = {
    					'duplicate_key': e.duplicate_key,
    					'complete': false,
    					'locked_at': null
    				};
    				var object = {
    					'$set': {
							'body': e.body,
							'inserted_at': nowutc,
							'complete': false,
              'locked' : false,
							'locked_till': null,
							'completed_at': null,
							'priority': e.priority,
							'duplicate_key': e.duplicate_key,
							'completecount': 0
						},
						'$inc': {'count': 1}
    				};
    		
    				db.#{collection.name}.update(query, object, true);
    			}
    			return ret;
    		}
        |
        cmd = BSON::OrderedHash.new
        cmd['$eval'] = js
        cmd['args'] = [@batch]
        cmd['nolock'] = true
		result = collection.db.command(cmd)
		@batch.clear
		#pp result
	end

	# Lock and return the next queue message if one is available. Returns nil if none are available. Be sure to
	# review the README.rdoc regarding proper usage of the locking process identifier (locked_by).
	# Example:
	#    doc = queue.pop()

	# {:body=>"foo", :id=>"4e039c372b70275e345206e4"}

	def pop(opts = {})
    timeout = opts[:timeout] || @config[:timeout]

    cmd = BSON::OrderedHash.new
    cmd['findandmodify'] = collection.name
    if timeout
      cmd['update'] = {'$set' => {:locked_till => Time.now.utc+timeout, :locked => true}}
      cmd['query']  = {:complete => false,
                       '$or'=>[ {:locked => false},
                                {:locked_till=> nil},
                                {:locked_till=>{'$lt'=>Time.now.utc}}] }
    else
      cmd['update'] = { '$set' => {:locked => true} }
      cmd['query']  = { :complete => false, :locked => false }
    end
    cmd['limit'] = 1
    cmd['new']   = true

    sort_directive               = BSON::OrderedHash.new
    sort_directive[:priority]    = Mongo::DESCENDING
    sort_directive[:inserted_at] = Mongo::ASCENDING
    cmd['sort']                  = sort_directive

    result = collection.db.command(cmd)

    if result['value']
      { :body => result['value']['body'],
        :id => result['value']['_id'].to_s }
    else
      nil
    end
  rescue Mongo::OperationFailure => of
    nil
  end

  # "Re-add" the document to the queue
  def unlock(id)
    begin
      cmd = BSON::OrderedHash.new
      cmd['findandmodify'] = collection.name
      cmd['query']         = {:_id => BSON::ObjectId.from_string(id)}
      cmd['update']        = {'$set' => {:locked => false, :locked_till => nil}}
      collection.db.command(cmd)
    rescue Mongo::OperationFailure => of
      nil
    end
  end

	# Remove the document from the queue. This should be called when the work is done and the document is no longer needed.
	# You must provide the process identifier that the document was locked with to complete it.
	def complete(id)
		begin
			cmd = BSON::OrderedHash.new
			cmd['findandmodify'] = collection.name
			cmd['query']         = {:_id => BSON::ObjectId.from_string(id)}
			cmd['update']        = {'$set' => {:completed_at => Time.now.utc, :complete => true}, '$inc' => {:completecount => 1} }
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
			      	var a = db.#{collection.name}.count({'complete': false, '$or':[{'locked' : false}, {'locked_till':null},{'locked_till':{'$lt':nowutc}}] });
			        var c = db.#{collection.name}.count({'complete': true});
			        var t = db.#{collection.name}.count();
			        var l = db.#{collection.name}.count({'complete': false, 'locked' : true, 'locked_till': {'$gte':nowutc} });
			        var rc = db.#{collection.name}.group({
			        	'key': {},
			        	'cond': {'complete':true},
			        	'$reduce': function(obj, prev){prev.count += (obj.completecount - 1);},
			        	'initial': {count: 0}
			        });
			        var p = db.#{collection.name}.group({
						'key': {'priority':1},
						'cond': {},
						'$reduce': function(obj, prev){if(obj.complete){prev.complete += 1;}else{prev.waiting += 1;}},
						'initial': {complete: 0, waiting:0}
					});
					var tasks = db.#{collection.name}.group({
						'key': {'body.task':1},
						'cond': {},
						'$reduce': function(obj, prev){if(obj.complete){prev.complete += 1;}else{prev.waiting += 1;}},
						'initial': {complete: 0, waiting:0}
					});

			        return [a, c, t, l, rc[0] ? rc[0].count : 0, p, tasks];
			      }
			    );
			  }"

		#possible additions

		#db.job_queue.group({
		#'key': {'priority':1},
		#'cond': {},
		#'$reduce': function(obj, prev){if(obj.complete){prev.complete += 1;}else{prev.waiting += 1;}},
		#'initial': {complete: 0, waiting:0}
		#});

		#db.job_queue.group({
		#'key': {'body.task':1},
		#'cond': {},
		#'$reduce': function(obj, prev){if(obj.complete){prev.complete += 1;}else{prev.waiting += 1;}},
		#'initial': {complete: 0, waiting:0}
		#});

		cmd = BSON::OrderedHash.new
        cmd['$eval'] = js
        cmd['nolock'] = true

		available, complete, total, locked, redundant_completes, priority, tasks =  collection.db.command(cmd)['retval']

		#available, complete, total, locked, redundant_completes, priority, tasks = collection.db.eval(js)
		
		{ :locked    => locked.to_i,
			:complete => complete.to_i,
			:available => available.to_i,
			:total     => total.to_i,
			:redundantcompletes => redundant_completes,
			:priority => priority,
			:tasks => tasks
		}
	end

	def self.generate_duplicate_key(body)
		return Digest::MD5.hexdigest(body) if body.class == "String"
		return Digest::MD5.hexdigest(body) if body.class == "Fixnum"
		#else
		return Digest::MD5.hexdigest(body.to_json) #won't ever match a duplicate. Need a better way to handle hashes and arrays.
	end

  def peek(opts = {})
    timeout = opts[:timeout] || @config[:timeout]
    query = {:complete => false, }

    if timeout
      query['$or'] = [ {:locked => false},
                       {:locked_till=> nil},
                       {:locked_till=>{'$lt'=>Time.now.utc}}]
    else
      query = {:locked => false}
    end

    collection.find( query, 
                    :sort => [[:priority, :descending],[:inserted_at, :ascending]],
                    :limit => 10)
  end

  # Set the priority of an item to a custom value
  def change_item_priority obj_id, priority
    cmd = BSON::OrderedHash.new
    cmd['findandmodify'] = collection.name
    cmd['update']        = { '$set' => { :priority => priority } }
    cmd['query']         = { '_id' => obj_id }

    collection.db.command(cmd)
  end

  # Increase the priority of an item by a given value
  def increase_item_priority obj_id, step=1
    cmd = BSON::OrderedHash.new
    cmd['findandmodify'] = collection.name
    cmd['update']        = { '$inc' => { :priority => step } }
    cmd['query']         = { '_id' => obj_id }

    collection.db.command(cmd)
  end

  # Decrease the priority of an item by a given value
  def decrease_item_priority obj_id, step=1
    cmd = BSON::OrderedHash.new
    cmd['findandmodify'] = collection.name
    cmd['update']        = { '$inc' => { :priority => - step } }
    cmd['query']         = { '_id' => obj_id }

    collection.db.command(cmd)
  end

	protected

	def value_of(result) #:nodoc:
		result['okay'] == 0 ? nil : result['value']
	end

end
